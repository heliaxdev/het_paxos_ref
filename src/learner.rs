use crate::{grpc::{acceptor_client::AcceptorClient,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            message_properties::{ballot,
                                 is_two_a_with_learner,
                                 signers,
                                 well_formed},
            parse_config::ParsedConfig,
            utils::hash};
use futures_util::{future::join_all, StreamExt};
use std::{borrow::BorrowMut,
          collections::HashMap,
          sync::{Arc, Condvar, Mutex}};
use tokio::{self, sync::mpsc::{unbounded_channel, UnboundedReceiver}};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;


/// represents each agent's internal configuration
pub struct LearnerState {
    config : ParsedConfig,
    known_messages : HashMap<Hash256, ConsensusMessage>,
}

impl LearnerState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    pub fn deliver_message<'a>(&'a mut self, message : ConsensusMessage, message_hash : &'a Hash256) {
        // if we have not received this message before:
        if !self.hash_received(message_hash) && self.is_well_formed(&message, message_hash) {

            println!("received {}", message_hash);

            self.known_messages.insert(message_hash.clone(), message);
            let known_messages : Box<dyn Fn(&Hash256) -> Option<&'a ConsensusMessage>> =
                Box::new(|x| self.known_messages.get(x));
            // check if anyone has decided anything, and if so, print it.
            for (learner, quorums) in self.config.learners.iter() {
                if is_two_a_with_learner(&self.config, &known_messages, learner, message_hash) {
                    let b = ballot(&known_messages, message_hash);
                    let signatures = signers(&self.config, &known_messages, 
                          self.known_messages.keys().filter(|reference| 
                                 ballot(&known_messages, reference) == b
                              && is_two_a_with_learner(&self.config, &known_messages, learner, reference)));
                    if quorums.iter().any(|quorum| quorum.iter().all(|acceptor| signatures.contains(acceptor))) {
                        println!("Learner {} decided in Ballot {:?}", learner, b);
                    }
                }
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

struct LearnerMutex {
    mutex : Mutex<LearnerState>,
    condvar : Condvar,
}


/// public struct representing an learner: completely opaque.
pub struct Learner(Arc<LearnerMutex>);


impl LearnerMutex {
    fn receive_message(&self, message : ConsensusMessage) {
        // wait until all preceeding messages are received, then deliver_message
        let message_hash = hash(&message);
        self.condvar.wait_while(self.mutex.lock().unwrap(), |learner_state| {
                !learner_state.predecessors_received(&message)
            }).unwrap().borrow_mut().deliver_message(message, &message_hash);
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
    // contact all the other servers, in parallel:
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
