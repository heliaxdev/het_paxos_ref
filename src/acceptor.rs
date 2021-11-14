use crate::{grpc::{acceptor_client::AcceptorClient,
                   acceptor_server::{self, AcceptorServer},
                   Ballot,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            parse_config::{ParsedAddress,
                           ParsedConfig,},
            utils::hash};
use itertools::Itertools;
use futures_core::{stream::Stream, task::{Context, Poll}};
use futures_util::StreamExt;
use std::{borrow::{Borrow, BorrowMut},
          collections::{HashMap, HashSet},
          iter::once,
          pin::Pin,
          slice::Iter,
          sync::{Arc, Condvar, Mutex},
          thread::sleep,
          time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{self, sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver}};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};


/// represents each agent's internal configuration
struct AcceptorState {
    config : ParsedConfig,
    known_messages : HashMap<Hash256, ConsensusMessage>,
    recent_messages : HashSet<Hash256>,
    out_channels : Vec<UnboundedSender<ConsensusMessage>>,
}

impl AcceptorState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    fn deliver_message(&mut self, message : ConsensusMessage) {
        let message_hash = hash(&message);
        // if we have not received this message before:
        if !self.hash_received(&message_hash) {
            // echo message to all other participants:
            for out_channel in &self.out_channels {
                out_channel.send(message.clone())
                    .expect("error while sending message to an out_channel");
            }
            self.known_messages.insert(message_hash.clone(), message.clone());
            self.recent_messages.insert(message_hash.clone());
            // TODO: determine which, if any, messages to generate (and then receive)
        }
    }

    fn hash_received(&self, message : &Hash256) -> bool {
        self.known_messages.get(message).is_some()
    }


    /// (Hashes of) all the messages transitively referenced by (the message with the hash of)
    /// reference.
    fn transitive_references<'a>(&'a self, reference : &'a Hash256) -> HashSet<&'a Hash256> {
        fn transitive_references_so_far<'a>(s : &'a AcceptorState,
                                             mut so_far : HashSet<&'a Hash256>,
                                             reference : &'a Hash256)
                                            -> HashSet<&'a Hash256> {
            if so_far.contains(reference) {
                so_far
            } else {
                so_far.insert(reference);
                if let Some(ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                             SignedHashSet{hash_set : Some(refs),..}))}) = s.known_messages.get(reference) {
                    refs.hashes.iter().fold(so_far, |w,r| transitive_references_so_far(s,w,r))
                } else {
                    so_far
                }
            }
        }
        transitive_references_so_far(self, HashSet::new(), reference)
    }

    /// Convert any collection of &Hash256 s into a Vec of ConsensusMessage s
    /// (with lifetime of self)
    fn into_messages<'a, 'b, T>(&'a self, hashes : T) -> Vec<&'a ConsensusMessage> 
            where T : IntoIterator<Item = &'b Hash256> {
        hashes.into_iter().filter_map(|r| self.known_messages.get(r)).collect()
    }

    /// Who (if anyone) has (correctly) signed this message?
    fn signer<'a>(&'a self, message : &ConsensusMessage) -> Option<&'a ParsedAddress> {
        if let ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                             SignedHashSet{hash_set : Some(hs), signature : Some(signature)}))} = message {
            for address in &self.config.known_addresses {
                if address.public_key.verify_signature(hs, signature.clone()) {
                    return Some(&address)
                }
            }
        }
        None
    }

    fn caught<'a>(&'a self, reference : &Hash256) -> HashSet<&'a ParsedAddress> {
        self.transitive_references(reference).iter().combinations(2).filter_map(|pair| {
            let reference_x = pair.get(0)?;
            let reference_y = pair.get(1)?;
            let message_x = self.known_messages.get(reference_x)?;
            let message_y = self.known_messages.get(reference_y)?;
            let signer_x = self.signer(message_x)?;
            let signer_y = self.signer(message_y)?;
            if   signer_x == signer_y
              && !self.transitive_references(reference_x).iter().contains(reference_y)
              && !self.transitive_references(reference_y).iter().contains(reference_x) {
                Some(signer_x)
            } else {
                None
            }
        }).collect()
    }

    fn connected_learners(&self,
                          learner_x : &ParsedAddress,
                          learner_y : &ParsedAddress,
                          caught : &HashSet<&ParsedAddress>) -> bool {
        // does this pair of learners have an uncaught acceptor in each of their quorum
        // intersections?
        if let (Some(quorums_x), Some(quorums_y)) =
               (self.config.learners.get(learner_x), self.config.learners.get(learner_y)) {
            for quorum_x in quorums_x {
                for quorum_y in quorums_y {
                    if caught.is_superset(&quorum_x.intersection(quorum_y).collect()) {
                        return false;
                    }
                }
            }
            return (quorums_x.len() > 0) && (quorums_y.len() > 0);
        }
        false
    }


    fn connected<'a>(&'a self, learner : &ParsedAddress, reference : &'a Hash256) -> HashSet<&'a ParsedAddress> {
        let caught_acceptors = self.caught(reference);
        self.config.known_addresses.iter().filter(|x| self.connected_learners(learner, x, &caught_acceptors)).collect()
    }

    fn buried(&self, learner : &ParsedAddress, reference : &Hash256, later_reference : &Hash256) -> bool {
        if let (Some(quorums), Some(v), b) =
            (self.config.learners.get(learner), self.value(reference), self.ballot(reference)) {
            let sigs = self.signers(self.transitive_references(later_reference).into_iter().filter(|m| {
                let refs = self.transitive_references(m);
                refs.contains(reference) && refs.iter().any(|z|
                    self.is_two_a_with_learner(learner, z)
                    && Some(v) != self.value(z)
                    && self.ballot(z) > b)
            }));
            return quorums.iter().any(|quorum| quorum.iter().all(|a| sigs.contains(a)));
        }
        false
    }

    fn signers<'a, 'b, T>(&'a self, references : T) -> HashSet<&'a ParsedAddress> 
            where T : IntoIterator<Item = &'b Hash256> {
        self.into_messages(references).iter().filter_map(|m| self.signer(m)).collect()
    }

    fn is_one_a(&self, reference : &Hash256) -> bool {
        if let Some(message) = self.known_messages.get(reference) {
            return message.is_one_a();
        }
        false
    }

    fn is_one_b(&self, reference : &Hash256) -> bool {
        if let Some(message) = self.known_messages.get(reference) {
            if let ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                     SignedHashSet{hash_set : Some(refs),..}))} = message {
                if let Some(one_a) = self.get_one_a(reference) {
                    // A OneB message is a direct response to a OneA
                    if refs.hashes.iter().contains(&hash(one_a)) {
                        return self.signer(message).is_some();
                    }
                }
            }
        }
        false
    }

    fn get_one_a<'a>(&'a self, reference : &'a Hash256) -> Option<&'a ConsensusMessage> {
        self.into_messages(self.transitive_references(reference)).into_iter()
            .filter(|m| m.is_one_a())
            .max_by_key(|m| match m {
                ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))} => Some(b),
                _ => None // since they're all one_as, this should never happen
            })
    }

    fn ballot<'a>(&'a self, reference : &'a Hash256) -> Option<&'a Ballot> {
        match self.get_one_a(reference) {
            Some(ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))}) => Some(&b),
            _ => None
        }
    }

    fn value<'a>(&'a self, reference : &'a Hash256) -> Option<&'a Hash256> {
        match self.ballot(reference) {
            Some(Ballot{value_hash : Some(h),..}) => Some(&h),
            _ => None
        }
    }

    fn fresh(&self, learner : &ParsedAddress, reference : &Hash256) -> bool {
        // TODO: actually write this
        true
    }

    fn quorum<'a>(&'a self, learner : &ParsedAddress, reference : &'a Hash256) -> HashSet<&'a Hash256> {
        let b = self.ballot(reference);
        self.transitive_references(reference).into_iter()
            .filter(|m| self.ballot(m) == b)
            .filter(|m| self.is_one_b(m))
            .filter(|m| self.fresh(learner, m))
            .collect()
    }

    fn is_two_a_with_learner(& self, learner : &ParsedAddress, reference : &Hash256) -> bool {
        if !self.is_one_b(reference) {
            if let Some(message) = self.known_messages.get(reference) {
                if let Some(sig) = self.signer(message) {
                    if let Some(quorums) = self.config.learners.get(learner) {
                        let q = self.signers(self.quorum(learner, reference));
                        return q.contains(sig) && quorums.iter().any(|qi| qi.iter().all(|a| q.contains(a)));
                    }
                }
            }
        }
        false
    }

    fn is_two_a(& self, reference : & Hash256) -> bool {
        self.config.learners.keys().any(|learner| self.is_two_a_with_learner(learner, reference))
    }

    fn well_formed(&self, reference : &Hash256) -> bool {
        self.is_one_a(reference) || self.is_one_b(reference) || self.is_two_a(reference)
    }


    fn predecessors_received(&self, message : &ConsensusMessage) -> bool {
       if let Some(MessageOneof::SignedHashSet(SignedHashSet{hash_set : Some(hashset),..})) =
               &message.message_oneof {
           hashset.hashes.iter().all(|message_hash| self.hash_received(message_hash))
       } else {
           true
       }
    }
}

struct AcceptorMutex {
    mutex : Mutex<AcceptorState>,
    condvar : Condvar,
}


/// public struct representing an acceptor: completely opaque.
pub struct Acceptor(Arc<AcceptorMutex>);


impl AcceptorMutex {
    fn receive_message(&self, message : ConsensusMessage) {
        // If it's a ballot, wait until the timetamp has passed
        if let Some(MessageOneof::Ballot(Ballot{timestamp : Some(timestamp), ..})) =
                &message.message_oneof {
            let timestamp_d = Duration::new(timestamp.seconds.try_into().unwrap(),
                                            timestamp.nanos.try_into().unwrap());
            let since_the_epoch = SystemTime::now()
                .duration_since(UNIX_EPOCH).expect("Time went backwards!?");
            if timestamp_d > since_the_epoch {
                sleep(timestamp_d - since_the_epoch);
            }
        }
        // wait until all preceeding messages are received, then deliver_message
        self.condvar.wait_while(self.mutex.lock().unwrap(), |acceptor_state| {
                !acceptor_state.predecessors_received(&message)
            }).unwrap().borrow_mut().deliver_message(message);
        self.condvar.notify_all();
    }

    fn add_out_channel(&self) -> UnboundedReceiver<ConsensusMessage> {
        // create a thing that will send response messages
        let (sender, receiver) = unbounded_channel();
        // begin atomic block:
        let mut guard = self.mutex.lock().unwrap();
        // send all known messages
        for message in guard.borrow().known_messages.values() {
            sender.send(message.clone()).unwrap_or_else(
                |e| eprintln!("problem sending a message in an UnboundedSender: {}", e))
        }
        // add it to the set of senders
        guard.borrow_mut().out_channels.push(sender);
        receiver
    }
}

/// public way to generate a new acceptor (it has received no messages) using a config file.
pub fn new_acceptor(config : ParsedConfig) -> Acceptor {
    Acceptor(Arc::new(AcceptorMutex{
               mutex : Mutex::new(AcceptorState {
                                      config,
                                      known_messages : HashMap::new(),
                                      recent_messages : HashSet::new(),
                                      out_channels : Vec::new(),
                                  }),
               condvar : Condvar::new(),
             }))
}

pub async fn launch_acceptor(config : ParsedConfig) -> Result<(), Box<dyn std::error::Error>> {
    let known_addresses = config.known_addresses.clone();
    let port = config.address.port.clone();
    let acceptor = new_acceptor(config);
    let acceptor_mutex_clone = acceptor.0.clone();
    // launch the server thread:
    let server_future = Server::builder().add_service(AcceptorServer::new(acceptor))
                          .serve(format!("[::1]:{}", port).parse()?);
    // contact all the other servers, in parallel:
    for address in known_addresses.into_iter() {
        let acceptor_mutex = acceptor_mutex_clone.clone();
        tokio::spawn(async move {
            match AcceptorClient::connect(format!("http://{}:{}", address.hostname, address.port)).await {
                Ok(mut client) => {
                    match client.stream_consensus_messages(Request::new(UnboundedReceiverStream::new(acceptor_mutex.add_out_channel()))).await {
                        Ok(response) => response.into_inner().for_each_concurrent(None, |message| async {
                            match message {
                                Ok(x) => acceptor_mutex.receive_message(x),
                                Err(y) => eprintln!("status error instead of message: {}", y)
                            }
                        }).await,
                        Err(x) => println!("{}:{} server returned error: {}", address.hostname, address.port, x),
                    }
                },
                Err(e) => println!("could not connect to {}:{}. Error: {}", address.hostname, address.port, e),
            };
        }); // TODO: Do I need an "await" here to ensure these connections actually launch?
    }
    server_future.await?;
    Ok(())
}

/// What follows is an ugly hack that exists only so I can have UnboundedReceiverStreams send Ts to
/// people who want Result<T,Status>s.
/// I really wish I could make this generic over the second type parameter of Result instead of
/// just Status.
pub struct UnboundedReceiverWrapper<T>(UnboundedReceiverStream<T>);
impl<T> Stream for UnboundedReceiverWrapper<T> {
    type Item = Result<T, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<T, Status>>> {
        let pn : Poll<Option<T>> = Pin::new(&mut self.0).poll_next(cx);
        match pn {
            Poll::Ready(Some(x)) => Poll::Ready(Some(Ok(x))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending
        }
    }
}

/// Implements the gRPC requirements for a LiarsLie Server
#[tonic::async_trait]
impl acceptor_server::Acceptor for Acceptor {
    /// streams messages to and/or from everyone else
    async fn stream_consensus_messages(&self, request: Request<Streaming<ConsensusMessage>>)
            -> Result<Response<Self::StreamConsensusMessagesStream>, Status> {
        // spawn a thread that will receive messages and act appropriately
        let acceptor_mutex = self.0.clone();
        tokio::spawn(async move {
            request.into_inner().for_each_concurrent(None, |message| async {
                match message {
                    Ok(x) => acceptor_mutex.receive_message(x),
                    Err(y) => eprintln!("status error instead of message: {}", y)
                }
            }).await;
        });
        Ok(Response::new(UnboundedReceiverWrapper(UnboundedReceiverStream::new(self.0.add_out_channel()))))
    }
    type StreamConsensusMessagesStream = UnboundedReceiverWrapper<ConsensusMessage>;
}

//TODO: code to start up an acceptor
