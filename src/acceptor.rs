
use crate::{grpc::{acceptor_server,
                   Ballot,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            utils::hash};
use futures_util::StreamExt;
use std::{borrow::{Borrow, BorrowMut},
          collections::{HashMap, HashSet},
          sync::{Arc, Condvar, Mutex},
          thread::sleep,
          time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{self, sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver}};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};


/// represents each agent's internal configuration
struct AcceptorState {
    known_messages : HashMap<Hash256, ConsensusMessage>,
    recent_messages : HashSet<Hash256>,
    out_channels : Vec<UnboundedSender<Result<ConsensusMessage, Status>>>,
}

impl AcceptorState { 
    /// CALLED ONLY WHEN YOU HAVE THE RELEVANT MUTEX
    fn deliver_message(&mut self, message : ConsensusMessage) {
        let message_hash = hash(&message);
        // if we have not received this message before:
        if !self.hash_received(&message_hash) {
            // echo message to all other participants:
            for out_channel in &self.out_channels {
                out_channel.send(Ok(message.clone()))
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

//    fn message_received(&self, message : &ConsensusMessage) -> bool {
//        self.hash_received(&hash(message))
//    }

    fn predecessors_received(&self, message : &ConsensusMessage) -> bool {
       if let Some(MessageOneof::SignedHashSet(SignedHashSet{hash_set : Some(hashset),..})) =
               &message.message_oneof {
           hashset.hashes.iter().all(|message_hash| self.hash_received(message_hash))
       } else {
           true
       }
    }
}

struct Acceptor {
    mutex : Mutex<AcceptorState>,
    condvar : Condvar,
}

impl Acceptor {
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

    fn add_out_channel(&self) -> UnboundedReceiver<Result<ConsensusMessage, Status>> {
        // create a thing that will send response messages
        let (sender, receiver) = unbounded_channel();
        // begin atomic block:
        let mut guard = self.mutex.lock().unwrap();
        // send all known messages
        for message in guard.borrow().known_messages.values() {
            sender.send(Ok(message.clone())).unwrap_or_else(
                |e| eprintln!("problem sending a message in an UnboundedSender: {}", e))
        }
        // add it to the set of senders
        guard.borrow_mut().out_channels.push(sender);
        receiver
    }
}

/// Implements the gRPC requirements for a LiarsLie Server
#[tonic::async_trait]
impl acceptor_server::Acceptor for Arc<Acceptor> {
    /// streams messages to and/or from everyone else
    async fn stream_consensus_messages(&self, request: Request<Streaming<ConsensusMessage>>)
            -> Result<Response<Self::StreamConsensusMessagesStream>, Status> {
        // spawn a thread that will receive messages and act appropriately
        let acceptor = self.clone();
        tokio::spawn(async move {
            request.into_inner().for_each_concurrent(None, |message| async {
                match message {
                    Ok(x) => acceptor.receive_message(x),
                    Err(y) => eprintln!("status error instead of message: {}", y)
                }
            }).await;
        });
        Ok(Response::new(UnboundedReceiverStream::new(self.add_out_channel())))
    }

    type StreamConsensusMessagesStream = UnboundedReceiverStream<Result<ConsensusMessage, Status>>;
}
