use crate::{grpc::{acceptor_client::AcceptorClient,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            parse_config::ParsedConfig,
            parsed_message::{ParsedMessage,
                             ParsedMessageRef},
            utils::hash};
use futures_util::{future::join_all, StreamExt};
use std::{borrow::BorrowMut,
          collections::{HashMap, HashSet},
          sync::{Arc, Condvar, Mutex}};
use tokio::{self, sync::mpsc::{unbounded_channel, UnboundedReceiver}};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;


/// represents each agent's internal configuration
/// Take care with concurrent state updates to ensure consistency.
/// Normally, we handle this by keeping one big lock, and only doing
///  state updated when we have the lock.
///
/// Optimization opportunity: as long as `known_messages` only ever
///     grows, we should not need to worry about inconsistencies in
///     the state, so we may not need a lock on all operations. 
///    However, currently we loop through `known_messages` looking for
///     quorums of 2as, which is tough if `known_messages` changes
///     during the loop.
pub struct LearnerState {
    /// The configuration of the Learner read from a file at startup. Do not change.
    config : ParsedConfig,
    /// All messages this Learner has ever received, indexed by hash. Should only grow. 
    known_messages : HashMap<Hash256, Arc<ParsedMessage>>,
}

impl LearnerState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    /// This is the core Learner behaviour. 
    /// Deliver a ConsensusMessage (received from an Acceptor via gRPC).
    /// This will check if the message has already been received, and is well-formed, and then:
    /// - print some debug info
    /// - add the message to `known_messages`
    /// - check to see if _any_ learner has decided on a value, and if so, print that.
    pub fn deliver_message(&mut self, message : ConsensusMessage) {
        let message_hash = hash(&message);
        // if we have not received this message before:
        if !self.hash_received(&message_hash) {
            // if the message is well-formed
            if let Some(message) = self.is_well_formed(message, message_hash.clone()) {

                // this whole block is just debugging printout
                println!("received message {}", &message_hash);
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

                // check to see if _any_ learner has decided on a value, and if so, print that.
                for (learner, quorums) in self.config.learners.iter() {
                    if message.is_two_a_with_learner(learner) {
                        let b = message.ballot();
                        let signatures : HashSet<_> = self.known_messages.values().filter(|m|
                                m.ballot() == b
                             && m.is_two_a_with_learner(learner)).filter_map(|m| m.signer().as_ref()).collect();
                        if quorums.iter().any(|quorum| quorum.iter().all(|acceptor| signatures.contains(acceptor))) {
                            println!("Learner {} decided in Ballot {:?}", learner, b);
                        }
                    }
                }

                // add the message to `known_messages`
                self.known_messages.insert(message_hash, message);
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

/// A mutex locking a LearnerState
/// The idea is that we'll use one big lock to totally serialize all
///  the interesting operations (which mostly means `deliver_message`).
struct LearnerMutex {
    mutex : Mutex<LearnerState>,
    condvar : Condvar,
}


/// public struct representing an learner: completely opaque.
/// Construct using new_learner.
/// Clones are Arcs referencing the same LearnerMutex, so you can clone it, but it's the same
///   mutex and state inside.
pub struct Learner(Arc<LearnerMutex>);


impl LearnerMutex {
    /// Called whenever a ConsensusMessage comes in over the wire. 
    /// - waits until message's references have already been delivered (this involves grabbing the
    ///    relevant mutex, checking, and releasing it)
    /// - Grabs the mutex and calls LearnerState::deliver_message.
    /// - Releases the mutex and notifies anyone who is waiting for more messages to be delivered.
    fn receive_message(&self, message : ConsensusMessage) {
        // wait until all preceeding messages are received, then deliver_message
        self.condvar.wait_while(self.mutex.lock().unwrap(), |learner_state| {
                !learner_state.predecessors_received(&message)
            }).unwrap().borrow_mut().deliver_message(message);
        self.condvar.notify_all();
    }

    /// Used when we open a channel to an `Acceptor` via gRPC.
    /// Represents a channel along which we can send
    ///  `ConsensusMessage`s, which are forwarded to the
    /// `Acceptor` over the wire via gRPC.
    /// Here, we do not store the `Sender` object, so we will never
    ///  send any messages. 
    /// However, we still need to return an
    ///  `UnboundedReceiver<ConsensusMessage>` object so gRPC can hold
    ///  it, and listen for messages we will never send.
    fn add_out_channel(&self) -> UnboundedReceiver<ConsensusMessage> {
        // create a thing that will send response messages
        let (_, receiver) = unbounded_channel();
        receiver
    }
}

/// public way to generate a new learner (it has received no messages) using a config file.
/// This does not connections to `Acceptor` servers.
/// It just creates a new acceptor object. 
pub fn new_learner(config : ParsedConfig) -> Learner {
    Learner(Arc::new(LearnerMutex{
               mutex : Mutex::new(LearnerState {
                                      config,
                                      known_messages : HashMap::new(),
                                  }),
               condvar : Condvar::new(),
             }))
}

/// Launches a Learner using a ParsedConfig.
/// This will establish connections to `Acceptor`s, so launch your
///  `Learner` _after_ your `Acceptor`s are launched.
/// This calls `new_learner'. 
/// This then interacts with gRPC generated code to connect to `Acceptor`s. 
pub async fn launch_learner(config : ParsedConfig) -> Result<(), Box<dyn std::error::Error>> {
    let known_addresses = config.known_addresses.clone();
    let learner = new_learner(config);
    let learner_mutex_clone = learner.0.clone();
    // launch the server thread:
    for await_me in join_all(known_addresses.into_iter().map(|address| {
         let learner_mutex = learner_mutex_clone.clone();
         tokio::spawn(async move {
             match AcceptorClient::connect(format!("http://{}:{}", address.hostname, address.port)).await {
                 Ok(mut client) => {
                     match client.stream_consensus_messages(Request::new(
                             UnboundedReceiverStream::new(learner_mutex.add_out_channel()))).await {
                         Ok(response) => response.into_inner().for_each_concurrent(None, |message| async {
                             match message {
                                 Ok(x) => learner_mutex.receive_message(x),
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
    })).await {
        await_me?;
    }
    Ok(())
}
