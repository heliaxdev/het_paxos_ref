use crate::{grpc::{acceptor_client::AcceptorClient,
                   Ballot,
                   ConsensusMessage,
                   consensus_message::MessageOneof},
            parse_config::ParsedConfig,
            utils::hash};
use pbjson_types::Timestamp;
use rand::{Rng, thread_rng};
use std::{thread::sleep,
          time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{self, sync::mpsc::unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;

/// A very simple proposer for Heterogeneous Paxos.
/// Connects to all the `Acceptors`, and then repeatedly proposes the
///  same value (with randomized exponential delays) forever.
/// Launch this _after_ all `Acceptor`s are launched, so it can
///  connect to them over gRPC.
/// Value proposed is taken from `config`.
/// (Well, it will be the hash of the value in `config`, but whatever).
pub async fn launch_proposer(config : ParsedConfig) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(proposal) = config.proposal {
        let value_hash = hash(&proposal);
        // contact all the acceptors, in parallel:
        let senders_and_threads = config.known_addresses.clone().into_iter().map(|address| {
            let (sender, receiver) = unbounded_channel();
            (sender,
            tokio::spawn(async move {
                match AcceptorClient::connect(format!("http://{}:{}", address.hostname, address.port)).await {
                    Ok(mut client) => {
                        match client.stream_consensus_messages(Request::new(
                                UnboundedReceiverStream::new(receiver))).await {
                            Ok(_) => println!("connection to {}:{} established", address.hostname, address.port),
                            Err(x) => println!("{}:{} server returned error: {}", address.hostname, address.port, x),
                        }
                    },
                    Err(e) => println!("could not connect to {}:{}. Error: {}", address.hostname, address.port, e),
                };
            }))
        }).collect::<Vec<_>>();

        // repeated proposals with exponential random timing
        for exponent in 0.. {
            sleep(Duration::new(thread_rng().gen_range(0..(2_u64.pow(exponent))), 0));
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
            let message = ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(
                Ballot{timestamp : Some(Timestamp{ // ballots use the current time by local clock
                    seconds : now.as_secs() as i64,
                    nanos : now.subsec_nanos() as i32 }),
                  value_hash : Some(value_hash.clone())}))};

            println!("proposing {}", &hash(&message));

            for (sender, _) in senders_and_threads.iter() {
                sender.send(message.clone())?;
            }
        }
        // nothing after this point should matter, because we should never get out of that loop.
        // However, imagining we somehow finished proposing, time to clean up all the connection
        // threads we made earlier. 
        for (_, await_me) in senders_and_threads.into_iter() {
            await_me.await?;
        }
    } else {
        eprintln!("no proposal found in my config!")
    }
    Ok(())
}
