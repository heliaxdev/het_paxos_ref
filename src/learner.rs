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
pub struct LearnerState {
    config : ParsedConfig,
    known_messages : HashMap<Hash256, Arc<ParsedMessage>>,
}

impl LearnerState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    pub fn deliver_message(&mut self, message : ConsensusMessage) {
        let message_hash = hash(&message);
        // if we have not received this message before:
        if !self.hash_received(&message_hash) {
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
                self.known_messages.insert(message_hash, message);
            }
        }
    }

    pub fn is_well_formed(&self, message : ConsensusMessage, hash : Hash256) -> Option<Arc<ParsedMessage>> {
        // create a known_messages lookup function that includes this given message
        // Alas, rust's type inference cannot figure out how to type this without help.
        let known_messages : Box<dyn Fn(&Hash256) -> Option<Arc<ParsedMessage>>> = 
            Box::new(move |h| self.known_messages.get(h).map(|x| x.clone()));
        ParsedMessage::new(message, hash, &self.config, &known_messages).map(Arc::new)
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

struct LearnerMutex {
    mutex : Mutex<LearnerState>,
    condvar : Condvar,
}


/// public struct representing an learner: completely opaque.
pub struct Learner(Arc<LearnerMutex>);


impl LearnerMutex {
    fn receive_message(&self, message : ConsensusMessage) {
        // wait until all preceeding messages are received, then deliver_message
        self.condvar.wait_while(self.mutex.lock().unwrap(), |learner_state| {
                !learner_state.predecessors_received(&message)
            }).unwrap().borrow_mut().deliver_message(message);
        self.condvar.notify_all();
    }

    fn add_out_channel(&self) -> UnboundedReceiver<ConsensusMessage> {
        // create a thing that will send response messages
        let (_, receiver) = unbounded_channel();
        receiver
    }
}

/// public way to generate a new learner (it has received no messages) using a config file.
pub fn new_learner(config : ParsedConfig) -> Learner {
    Learner(Arc::new(LearnerMutex{
               mutex : Mutex::new(LearnerState {
                                      config,
                                      known_messages : HashMap::new(),
                                  }),
               condvar : Condvar::new(),
             }))
}

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
