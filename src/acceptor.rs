use crate::{grpc::{self,
                   acceptor_client::AcceptorClient,
                   acceptor_server::{self, AcceptorServer},
                   Ballot,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            parse_config::ParsedConfig,
            parsed_message::{ParsedMessage,
                             ParsedMessageRef},
            utils::hash};
use futures_core::{stream::Stream, task::{Context, Poll}};
use futures_util::{future::join_all, join, StreamExt};
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
/// Take care with concurrent state updates to ensure consistency.
/// Normally, we handle this by keeping one big lock, and only doing
///  state updated when we have the lock.
pub struct AcceptorState {
    /// The configuration of the Acceptor read from a file at startup. Do not change.
    config : ParsedConfig,
    /// All messages this Acceptor has ever received, indexed by hash. Should only grow. 
    known_messages : HashMap<Hash256, Arc<ParsedMessage>>,
    /// Hashes of messages received since this Acceptor last sent a message.
    /// Grows each time we receive a message.
    /// Resets to empty when we send a message.
    recent_messages : HashSet<Hash256>,
    /// Send a ConsensusMessage on these to send it out to another Acceptor or Learner. 
    /// These should be added whenever an incoming request opens an RPC with this server, or
    /// whenever our local clients open an RPC with another server (which we will try to do with
    /// all acceptors at startup). 
    out_channels : Vec<UnboundedSender<ConsensusMessage>>,
}

impl AcceptorState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    /// CALLED ONLY WHEN ALL REFERENCES IN THE DELIVERED MESSAGE HAVE ALREADY BEEN RECEIVED
    /// This is the core consensus algorithm step. 
    /// Deliver a ConsensusMessage (received from a Proposer or Acceptor via gRPC).
    /// This will check if the message has already been received, and is well-formed, and then:
    /// - print some debug info
    /// - echo the message along all outgoing channels
    /// - add the message to self.known_messages
    /// - add the message to self.recent_messagse
    /// - make a new outgoing message (referencing self.recent_messages), and if that is
    ///     well-formed, deliver it as well (recurse)
    pub fn deliver_message(&mut self, message : ConsensusMessage) {
        let message_hash = hash(&message);
        // if we have not received this message before:
        if !self.hash_received(&message_hash) {
            // if this message is well-formed:
            if let Some(message) = self.is_well_formed(message, message_hash.clone()) {

                // print some debug information
                println!("forwarding message {}", &message_hash);
                if let Some(address) = message.signer() {
                    println!("  from: {}", &address.name);
                }
                if message.is_one_a() {
                    println!("  1A ballot: {:?}", &message.ballot());
                }
                if message.is_one_b() {
                    for learner in self.config.learners.keys() {
                        println!("  1B fresh for {}: {}", learner, message.fresh(learner));
                    }
                }
                if message.is_two_a() {
                    for learner in self.config.learners.keys() {
                        println!("  2A for learner {}: {}", learner, message.is_two_a_with_learner(learner));
                    }
                }


                // echo message to all other participants:
                for out_channel in &self.out_channels {
                    out_channel.send(message.original().clone())
                      .unwrap_or_else(|_| eprintln!("error while sending message to an out_channel"));
                }

                // add message to known_messages and recent_messages
                self.known_messages.insert(message_hash.clone(), message);
                self.recent_messages.insert(message_hash);
                // Optimization opportunity (not implemented here):
                // We could remove anything in message.transitive_references() from recent_messages.
                
                // make a new outgoing message (referencing self.recent_messages), and if that is
                //  well-formed, deliver it as well (recurse)
                let next_message_hash_set = grpc::HashSet{hashes : self.recent_messages.iter().cloned().collect()};
                let next_message = ConsensusMessage{message_oneof : Some(
                    MessageOneof::SignedHashSet(SignedHashSet{
                        signature : Some(self.config.private_key.sign_message(&next_message_hash_set)),
                        hash_set : Some(next_message_hash_set)
                    }))};
                let next_message_hash = hash(&next_message);
                if  self.is_well_formed(next_message.clone(), next_message_hash.clone()).is_some() {

                    println!("sending message {} referencing:", &next_message_hash);
                    for recent_message_hash in self.recent_messages.iter() {
                        println!("    {}", &recent_message_hash);
                    }

                    self.recent_messages.clear();
                    self.deliver_message(next_message);
                    // Optimization opportunity (not implemented here):
                    // We can avoid double-checking is_well_formed on our own messages if we pass
                    //   along some indication it's from self. 
                }
            }
        }
    }

    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    /// Determine if this message is well-formed using the ParsedMessage::new constructor.
    /// Returns None if the message is not well-formed.
    /// Returns Some(Arc(ParsedMessage)) if it is well-formed.
    /// This uses self.known_messages to look up messages corresponding to the reference hashes in
    ///   message.
    pub fn is_well_formed(&self, message : ConsensusMessage, hash : Hash256) -> Option<Arc<ParsedMessage>> {
        // create a known_messages lookup function using known_messages
        // Alas, rust's type inference cannot figure out how to type this without help.
        let known_messages : Box<dyn Fn(&Hash256) -> Option<Arc<ParsedMessage>>> = 
            Box::new(move |h| self.known_messages.get(h).map(|x| x.clone()));
        ParsedMessage::new(message, hash, &self.config, &known_messages).map(Arc::new)
    }

    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    /// Have we received a message with this hash before?
    /// Uses self.known_messages.
    pub fn hash_received(&self, message : &Hash256) -> bool {
        self.known_messages.get(message).is_some()
    }

    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    /// Have all references in message already been received?
    /// Uses self.known_messages.
    pub fn predecessors_received(&self, message : &ConsensusMessage) -> bool {
       if let Some(MessageOneof::SignedHashSet(SignedHashSet{hash_set : Some(hashset),..})) =
               &message.message_oneof {
           return hashset.hashes.iter().all(|message_hash| self.hash_received(message_hash));
       }
       true
    }
}

/// A mutex locking an AcceptorState
/// The idea is that we'll use one big lock to totally serialize all
///  the interesting operations (which mostly means `deliver_message`).
struct AcceptorMutex {
    mutex : Mutex<AcceptorState>,
    condvar : Condvar,
}


/// public struct representing an acceptor: completely opaque.
/// Construct using new_acceptor.
/// Clones are Arcs referencing the same AcceptorMutex, so you can clone it, but it's the same
///   mutex and state inside.
pub struct Acceptor(Arc<AcceptorMutex>);


impl AcceptorMutex {
    /// Called whenever a ConsensusMessage comes in over the wire. 
    /// - waits until the message's timestamp is in the past (by local clock).
    /// - waits until message's references have already been delivered (this involves grabbing the
    /// relevant mutex, checking, and releasing it)
    /// - Grabs the mutex and calls AcceptorState::deliver_message.
    /// - Releases the mutex and notifies anyone who is waiting for more messages to be delivered.
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

    /// Adds a new outgoing channel that will receive copies all the messages this Acceptor has *ever*
    /// received. 
    /// Note that this means it immediately receives all the messages this Acceptor has already
    /// received, and then receives a new message with every call to AcceptorState::deliver_message.
    /// This is called whenever we establish a new connection (of either direction) to anyone else. 
    /// This does involve temporarily grabbing the AcceptorMutex. 
    fn add_out_channel(&self) -> UnboundedReceiver<ConsensusMessage> {
        // create a thing that will send response messages
        let (sender, receiver) = unbounded_channel();
        // begin atomic block:
        let mut guard = self.mutex.lock().unwrap();
        // send all known messages
        for message in guard.borrow().known_messages.values() {
            sender.send(message.original().clone()).unwrap_or_else(
                |e| eprintln!("problem sending a message in an UnboundedSender: {}", e))
        }
        // add it to the set of senders
        guard.borrow_mut().out_channels.push(sender);
        receiver
    }
}

/// public way to generate a new acceptor (it has received no messages) using a config file.
/// This does not launch a server.
/// It just creates a new acceptor object. 
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

/// Launches an Acceptor server using a ParsedConfig.
/// This calls `new_acceptor`. 
/// This then interacts with gRPC generated code to start up the server. 
pub async fn launch_acceptor(config : ParsedConfig) -> Result<(), Box<dyn std::error::Error>> {
    let known_addresses = config.known_addresses.clone();
    let port = config.address.port.clone();
    let acceptor = new_acceptor(config);
    let acceptor_mutex_clone = acceptor.0.clone();
    // launch the server thread:
    let (r, vr) = join!(Server::builder().add_service(AcceptorServer::new(acceptor))
              .serve(format!("[::1]:{}", port).parse()?),
          // contact all the other servers, in parallel:
          join_all(known_addresses.into_iter().map(|address| {
              let acceptor_mutex = acceptor_mutex_clone.clone();
              tokio::spawn(async move {
                  match AcceptorClient::connect(format!("http://{}:{}", address.hostname, address.port)).await {
                      Ok(mut client) => {
                          match client.stream_consensus_messages(Request::new(
                                  UnboundedReceiverStream::new(acceptor_mutex.add_out_channel()))).await {
                              Ok(response) => response.into_inner().for_each_concurrent(None, |message| async {
                                  match message {
                                      Ok(x) => acceptor_mutex.receive_message(x),
                                      // Note, when someone shuts down, we get A LOT of these.
                                      Err(y) => eprintln!("status error instead of message: {}", y)
                                  }
                              }).await,
                              Err(x) => println!("{}:{} server returned error: {}", address.hostname, address.port, x),
                          }
                      },
                      Err(e) => println!("could not connect to {}:{}. Error: {}", address.hostname, address.port, e),
                  };
              })
          })));
    r?;
    for r in vr {
        r?;
    }
    Ok(())
}

/// What follows is an ugly hack that exists only so I can have UnboundedReceiverStreams send Ts to
/// things that want Result<T,Status>s.
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

/// Implements the gRPC requirements for an Acceptor Server
/// Each stream_consensus_message message receipt calls receive_message, and all messages sent by
/// deliver_message will be sent back along the return connection. 
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
