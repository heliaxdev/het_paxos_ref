use crate::{grpc::{self,
                   acceptor_client::AcceptorClient,
                   acceptor_server::{self, AcceptorServer},
                   Ballot,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            message_properties::well_formed,
            parse_config::ParsedConfig,
            utils::hash};
use futures_core::{stream::Stream, task::{Context, Poll}};
use futures_util::StreamExt;
use std::{borrow::{Borrow, BorrowMut},
          collections::{HashMap, HashSet},
          pin::Pin,
          sync::{Arc, Condvar, Mutex},
          thread::sleep,
          time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{self, sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver}};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};


/// represents each agent's internal configuration
pub struct AcceptorState {
    config : ParsedConfig,
    known_messages : HashMap<Hash256, ConsensusMessage>,
    recent_messages : HashSet<Hash256>,
    out_channels : Vec<UnboundedSender<ConsensusMessage>>,
}

impl AcceptorState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    pub fn deliver_message(&mut self, message : ConsensusMessage) {
        let message_hash = hash(&message);
        // if we have not received this message before:
        if !self.hash_received(&message_hash) && self.is_well_formed(&message, &message_hash) {

            println!("forwarding message {}", &message_hash);

            // echo message to all other participants:
            for out_channel in &self.out_channels {
                out_channel.send(message.clone())
                    .expect("error while sending message to an out_channel");
            }
            self.known_messages.insert(message_hash.clone(), message);
            self.recent_messages.insert(message_hash);
            let next_message_hash_set = grpc::HashSet{hashes : self.recent_messages.iter().cloned().collect()};
            let next_message = ConsensusMessage{message_oneof : Some(
                MessageOneof::SignedHashSet(SignedHashSet{
                    signature : Some(self.config.private_key.sign_message(&next_message_hash_set)),
                    hash_set : Some(next_message_hash_set)
                }))};
            let next_message_hash = hash(&next_message);
            if self.is_well_formed(&next_message, &next_message_hash) {

                println!("sending message {} referencing:", &next_message_hash);
                for recent_message_hash in self.recent_messages.iter() {
                    println!("    {}", &recent_message_hash);
                }

                self.recent_messages.clear();
                self.deliver_message(next_message);
                // note: we can avoid double-checking is_well_formed on our own messages if we pass
                // along some indication it's from self. 
                // However, this is an optimization, so I'm not doing it here.
            }
        }
    }

    pub fn is_well_formed<'a>(&'a self, message : &'a ConsensusMessage, hash : &'a Hash256) -> bool {
        // create a known_messages lookup function that includes this given message
        // Alas, rust's type inference cannot figure out how to type this without help.
        let known_messages : Box<dyn Fn(&Hash256) -> Option<&'a ConsensusMessage>> = 
            Box::new(move |h| if h == hash {Some(message)} else {self.known_messages.get(h)});
        // use that to look up if this message is well-formed.
        well_formed(&self.config, &known_messages, &hash)
    }

    pub fn hash_received(&self, message : &Hash256) -> bool {
        self.known_messages.get(message).is_some()
    }

    pub fn predecessors_received(&self, message : &ConsensusMessage) -> bool {
       if let Some(MessageOneof::SignedHashSet(SignedHashSet{hash_set : Some(hashset),..})) =
               &message.message_oneof {
           return hashset.hashes.iter().all(|message_hash| self.hash_received(message_hash));
       }
       true
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
    for await_me in known_addresses.into_iter().map(|address| {
        let acceptor_mutex = acceptor_mutex_clone.clone();
        tokio::spawn(async move {
            match AcceptorClient::connect(format!("http://{}:{}", address.hostname, address.port)).await {
                Ok(mut client) => {
                    match client.stream_consensus_messages(Request::new(
                            UnboundedReceiverStream::new(acceptor_mutex.add_out_channel()))).await {
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
        })
    }).collect::<Vec<_>>() {
        await_me.await?;
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
