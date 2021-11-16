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




/// (Hashes of) all the messages transitively referenced by (the message with the hash of)
/// reference.
pub fn transitive_references<'a>(known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>),
                                 reference : &'a Hash256)
                                 -> HashSet<&'a Hash256> {
    fn transitive_references_so_far<'a>(known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>),
                                         mut so_far : HashSet<&'a Hash256>,
                                         reference : &'a Hash256)
                                        -> HashSet<&'a Hash256> {
        if so_far.insert(reference) {
            if let Some(ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                         SignedHashSet{hash_set : Some(refs),..}))}) = known_messages(reference) {
                return refs.hashes.iter().fold(so_far, |w,r| transitive_references_so_far(known_messages,w,r));
            }
        }
        so_far
    }
    transitive_references_so_far(known_messages, HashSet::new(), reference)
}
/// Convert any collection of &Hash256 s into a Vec of ConsensusMessage s
/// (with lifetime of self)
pub fn into_messages<'a, 'b, T>(known_messages : &(fn(&Hash256) -> Option<&'a ConsensusMessage>),
                                hashes : T)
                                -> Vec<&'a ConsensusMessage> 
        where T : IntoIterator<Item = &'b Hash256> {
    hashes.into_iter().filter_map(|r| known_messages(r)).collect()
}

pub fn signed_by<'b, 'd>(known_messages : &(fn(&'d Hash256) -> Option<&'b ConsensusMessage>),
                         address : &ParsedAddress,
                         reference : &'d Hash256)
                         -> bool {
    if let Some(message) = known_messages(reference) {
        return address.signed(message);
    }
    false
}

/// Who (if anyone) has (correctly) signed this message?
pub fn signer<'a, 'b, 'd>(config : &'a ParsedConfig,
                          known_messages : &(fn(&'d Hash256) -> Option<&'b ConsensusMessage>),
                          reference : &'d Hash256)
                          -> Option<&'a ParsedAddress> {
    for address in &config.known_addresses {
        if signed_by(known_messages, &address, reference) {
            return Some(&address);
        }
    }
    None
}

pub fn caught<'a, 'b>(config : &'a ParsedConfig,
                      known_messages : &(fn(&'b Hash256) -> Option<&'b ConsensusMessage>),
                      reference : &'b Hash256)
                      -> HashSet<&'a ParsedAddress> {
    transitive_references(known_messages, reference).iter().combinations(2).filter_map(|pair| {
        let reference_x = pair.get(0)?;
        let reference_y = pair.get(1)?;
        let s = signer(config, known_messages, reference_x)?;
        if   signed_by(known_messages, s, reference_y)
          && !transitive_references(known_messages, reference_x).iter().contains(reference_y)
          && !transitive_references(known_messages, reference_y).iter().contains(reference_x) {
            return Some(s);
        }
        None
    }).collect()
}

pub fn connected_learners(config : &ParsedConfig,
                          learner_x : &ParsedAddress,
                          learner_y : &ParsedAddress,
                          caught : &HashSet<&ParsedAddress>)
                          -> bool {
    // does this pair of learners have an uncaught acceptor in each of their quorum
    // intersections?
    if let (Some(quorums_x), Some(quorums_y)) =
           (config.learners.get(learner_x), config.learners.get(learner_y)) {
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


pub fn connected<'a, 'b>(config : &'a ParsedConfig,
                         known_messages : &(fn(&'b Hash256) -> Option<&'b ConsensusMessage>),
                         learner : &ParsedAddress,
                         reference : &'b Hash256)
                         -> HashSet<&'a ParsedAddress> {
    let caught_acceptors = caught(config, known_messages, reference);
    config.learners.keys().filter(|x| connected_learners(config, learner, x, &caught_acceptors)).collect()
}

pub fn buried<'b>(config : &ParsedConfig,
                  known_messages : &(fn(&'b Hash256) -> Option<&'b ConsensusMessage>),
                  learner : &ParsedAddress,
                  reference : &'b Hash256,
                  later_reference : &'b Hash256)
                  -> bool {
    if let (Some(quorums), Some(v), b) =
        (config.learners.get(learner), value(known_messages, reference), ballot(known_messages, reference)) {
        let sigs = signers(config,
                           known_messages,
                           transitive_references(known_messages, later_reference).into_iter().filter(|m| {
            let refs = transitive_references(known_messages, m);
            refs.contains(reference) && refs.iter().any(|z|
                is_two_a_with_learner(config, known_messages, learner, z)
                && Some(v) != value(known_messages, z)
                && ballot(known_messages, z) > b)
        }));
        return quorums.iter().any(|quorum| quorum.iter().all(|a| sigs.contains(a)));
    }
    false
}

pub fn connected_two_as<'a>(config : &ParsedConfig,
                            known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>),
                            learner : &ParsedAddress,
                            reference : &'a Hash256)
                            -> HashSet<&'a Hash256> {
    if let Some(sig) = signer(config, known_messages, reference) {
        return transitive_references(known_messages, reference).into_iter().filter(|m|
                   connected(config, known_messages, learner, reference).into_iter().any(|lrn|
                          signed_by(known_messages, sig, m)
                       && is_two_a_with_learner(config, known_messages, lrn, m)
                       && !buried(config, known_messages, lrn, m, reference))).collect();
    }
    HashSet::new()
}

pub fn signers<'a, 'b, 'c, T>(config : &'a ParsedConfig,
                              known_messages : &(fn(&'c Hash256) -> Option<&'b ConsensusMessage>),
                              references : T)
                              -> HashSet<&'a ParsedAddress> 
        where T : IntoIterator<Item = &'c Hash256> {
    references.into_iter().filter_map(|m| signer(config, known_messages, m)).collect()
}

pub fn is_one_a<'a, 'b>(known_messages : &(fn(&'a Hash256) -> Option<&'b ConsensusMessage>), reference : &'a Hash256)
    -> bool {
    if let Some(message) = known_messages(reference) {
        return message.is_one_a();
    }
    false
}

pub fn is_one_b<'a>(config : &ParsedConfig,
                    known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>),
                    reference : &'a Hash256)
                    -> bool {
    if let Some(ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                   SignedHashSet{hash_set : Some(refs),..}))}) = known_messages(reference) {
        if let Some(one_a) = get_one_a(known_messages, reference) {
            // A OneB message is a direct response to a OneA
            if refs.hashes.iter().contains(one_a) {
                return signer(config, known_messages, reference).is_some();
            }
        }
    }
    false
}

pub fn get_one_a<'a>(known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>), reference : &'a Hash256)
    -> Option<&'a Hash256> {
    transitive_references(known_messages, reference).into_iter()
        .filter(|m| is_one_a(known_messages, m))
        .max_by_key(|m| match known_messages(m) {
            Some(ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))}) => Some(b),
            _ => None // since they're all one_as, this should never happen
        })
}

pub fn ballot<'a>(known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>), reference : &'a Hash256)
    -> Option<&'a Ballot> {
    if let Some(ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))}) =
            known_messages(get_one_a(known_messages, reference)?) {
        return Some(&b);
    }
    None
}

pub fn value<'a>(known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>), reference : &'a Hash256)
    -> Option<&'a Hash256> {
    ballot(known_messages, reference)?.value_hash.as_ref()
}

pub fn fresh<'b>(config : &ParsedConfig,
                 known_messages : &(fn(&'b Hash256) -> Option<&'b ConsensusMessage>),
                 learner : &ParsedAddress,
                 reference : &'b Hash256)
                 -> bool {
    let v = value(known_messages, reference);
    connected_two_as(config, known_messages, learner, reference).into_iter().all(|m| value(known_messages, m) == v)
}

pub fn quorum<'a>(config : &ParsedConfig,
                  known_messages : &(fn(&'a Hash256) -> Option<&'a ConsensusMessage>),
                  learner : &ParsedAddress,
                  reference : &'a Hash256)
                  -> HashSet<&'a Hash256> {
    let b = ballot(known_messages, reference);
    transitive_references(known_messages, reference).into_iter()
        .filter(|m| ballot(known_messages, m) == b)
        .filter(|m| is_one_b(config, known_messages, m))
        .filter(|m| fresh(config, known_messages, learner, m))
        .collect()
}

pub fn is_two_a_with_learner<'b>(config : &ParsedConfig,
                                 known_messages : &(fn(&'b Hash256) -> Option<&'b ConsensusMessage>),
                                 learner : &ParsedAddress,
                                 reference : &'b Hash256)
                                 -> bool {
    if !is_one_b(config, known_messages, reference) {
        if let Some(sig) = signer(config, known_messages, reference) {
            if let Some(quorums) = config.learners.get(learner) {
                let q = signers(config, known_messages, quorum(config, known_messages, learner, reference));
                let b = ballot(known_messages, reference);
                return    q.contains(sig) // we signed a 1b in q
                       // and there is a quorum for this learner of which q is a superset.
                       && quorums.iter().any(|qi| qi.iter().all(|a| q.contains(a)))
                       // and there are no messages (other than this one) in this messages'
                       // transitive references that are already 2As with this signer, ballot,
                       // and learner.
                       && transitive_references(known_messages, reference).into_iter().all(|r|
                              r == reference
                           || !signed_by(known_messages, sig, r)
                           || ballot(known_messages, r) != b
                           || !is_two_a_with_learner(config, known_messages, learner, r));
            }
        }
    }
    false
}

pub fn is_two_a<'b>(config : &ParsedConfig,
                    known_messages : &(fn(&Hash256) -> Option<&'b ConsensusMessage>),
                    reference : & Hash256)
                    -> bool {
    config.learners.keys().any(|learner| is_two_a_with_learner(config, known_messages, learner, reference))
}

pub fn well_formed<'b>(config : &ParsedConfig,
                       known_messages : &(fn(&Hash256) -> Option<&'b ConsensusMessage>),
                       reference : &Hash256)
                       -> bool {
       is_one_a(known_messages, reference)
    || is_one_b(config, known_messages, reference)
    || is_two_a(config, known_messages, reference)
}
