use crate::{config::{Address, Config, MinimalQuorums},
            crypto::{PublicKey, PrivateKey},
            grpc::{ConsensusMessage,
                   consensus_message::MessageOneof,
                   SignedHashSet},
           };
use serde_json;
use std::{collections::{HashMap, HashSet},
          sync::Arc,
};


/// Used to refer to servers (such as `Acceptor`s) and other actors.
/// Contains everything you need to contact an actor, or verify its signatures.
/// We expect to read these from Config files.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ParsedAddress {
    /// Shorthand name of the address (used in printouts and config files)
    pub name : String,
    /// hostname (or IP address) of the address (used in establishing gRPC connections)
    pub hostname : String,
    /// IPv4 port of the address (used in establishing gRPC connections)
    pub port : u32,
    /// public key used to verify signatures by this address.
    pub public_key : PublicKey,
}

impl ParsedAddress {
    /// Did this Address sign this `ConsensusMessage`?
    /// Note that this really only applies to `SignedHashSet` messages.
    /// For all other messages, this will naturally return `false`.
    pub fn signed(&self, message : &ConsensusMessage) -> bool {
        if let ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                             SignedHashSet{hash_set : Some(hs), signature : Some(signature)}))} = message {
            return self.public_key.verify_signature(hs, signature.clone());
        }
        false
    }
}

/// Represents a Config file that has been fully parsed, and is ready
///  to be used in a `Learner`, `Acceptor`, or `Proposer`.
/// We use a lot of `Arc`s here so cloning values takes constant time.
/// They should not be able to produce reference loops. 
/// Construct one of these from a JSON file with `from_json`.
pub struct ParsedConfig {
    /// The set of known addresses of all actors (including `Acceptors`).
    /// `Learner`s, `Acceptor`s, and `Proposer`s will want to
    ///  establish gRPC connections to _all_ of these.
    pub known_addresses : Vec<Arc<ParsedAddress>>,
    /// If this is a proposer, what value should it (hash and then) propose?
    pub proposal : Option<String>,
    /// Private key this actor should use for signatures
    /// Note that this should match a public key among `known_addresses`.
    /// Technically, `Learners` and `Proposers` don't need a
    ///  `private_key` or `address`, but for simplicity, we're making
    ///  everyone have both.
    pub private_key : PrivateKey,
    /// `ParsedAddress` of this server.
    /// Note that this should point to a `ParsedAddress` present in
    ///  `known_addresses`.
    /// Technically, `Learners` and `Proposers` don't need a
    ///  `private_key` or `address`, but for simplicity, we're making
    ///  everyone have both.
    pub address : Arc<ParsedAddress>,
    /// Describes the quorums of each learner.
    /// Learners are identified by `String`s from the Config file.
    /// Quorums are represented as HashSets of `ParsedAddress`s.
    pub learners : HashMap<Arc<String>, Vec<HashSet<Arc<ParsedAddress>>>>,
    /// Describes the safety sets of each (unordered) pair of learners.
    /// Learner pairs are identified by (lexicographically ordered)
    ///  pairs of `String`s from the Config file.
    /// (Minimal) Safety Sets are represented as HashSets of `ParsedAddress`s.
    pub safety_sets : HashMap<(Arc<String>,Arc<String>) , Vec<HashSet<Arc<ParsedAddress>>>>,
}


/// Parse a `ParsedConfig` from a JSON `str`.
/// Can fail if any number of parsing problems happen.
/// I think it actually panics if config's private key doesn't match a
///  public key from `known_address`.
pub fn from_json(s: &str) -> serde_json::error::Result<ParsedConfig> {
    let Config { private_key : private_key_string,
                 proposal : proposal_string,
                 learners : grpc_learners,
                 addresses : grpc_addresses,
                 safety_sets : grpc_safety_sets } = serde_json::from_str(s)?;
    let known_addresses : Vec<Arc<ParsedAddress>> = grpc_addresses.into_iter().map(
        |Address{name, hostname, port, public_key}| 
            Arc::new(ParsedAddress {
                name,
                hostname,
                port,
                public_key : PublicKey::new_default_scheme(public_key),
            })).collect();
    let addresses_by_name : HashMap<String, Arc<ParsedAddress>> = known_addresses.iter().map(
        |address| (address.name.clone(), address.clone())).collect();
    let learners_by_name : HashMap<String, Arc<String>> = grpc_learners.iter().map(
        |(name, _)| (name.clone(), Arc::new(name.clone())) ).collect();
    let private_key = PrivateKey::new_default_scheme(private_key_string);
    // Optimization Opportunity:
    // there has got to be a better way to identify which `PublicKey` matches this `PrivateKey`.
    let signature = private_key.sign_bytes(&vec![1,2,3,4,5][..]);
    let address = known_addresses.iter().find(
        |p| p.public_key.verify_bytes(&vec![1,2,3,4,5][..], signature.clone())
        ).expect("my private key did not match any known public key").clone();
    let get_address = |name : &String|
        addresses_by_name.get(name).expect(&format!("name {} not found in addresses", name)).clone();
    let get_learner = |name : &String|
        learners_by_name.get(name).expect(&format!("name {} not found in learners", name)).clone();
    let fill_quorums = |mqs : &MinimalQuorums|  mqs.quorums.iter().map(
        |q| q.names.iter().map(get_address).collect()).collect();
    let learners = grpc_learners.iter().map(
        |(learner_name, mqs)| (get_learner(learner_name), fill_quorums(mqs))).collect();
    let ordered = | a : Arc<String>, b : Arc<String> | if a<b { (a,b) } else { (b,a) };
    let safety_sets = grpc_safety_sets.iter().map(|(learner_name_0, edges)|
        edges.safety_sets.iter().map(|(learner_name_1, mqs)|
          (ordered(get_learner(learner_name_0), get_learner(learner_name_1)), fill_quorums(mqs)))
        ).flatten().collect();
    let proposal = if proposal_string.len() > 0 {Some(proposal_string)} else {None};
    Ok(ParsedConfig{
        known_addresses,
        proposal,
        private_key,
        address,
        learners,
        safety_sets,
       })
}
