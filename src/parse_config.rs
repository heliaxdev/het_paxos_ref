use crate::{config::{Address, Config},
            crypto::{PublicKey, PrivateKey},
            grpc::{ConsensusMessage,
                   consensus_message::MessageOneof,
                   SignedHashSet},
           };
use serde_json;
use std::collections::{HashMap, HashSet};

// We're going to end up copying ParsedAddress objects A LOT.
// I'd like to use something more clever.
// Maybe some kind of pointer that lasts as long as the Config exists.
// Not even sure how to do that...

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ParsedAddress {
    pub name : String,
    pub hostname : String,
    pub port : u32,
    pub public_key : PublicKey,
}

impl ParsedAddress {
    pub fn signed(&self, message : &ConsensusMessage) -> bool {
        if let ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                             SignedHashSet{hash_set : Some(hs), signature : Some(signature)}))} = message {
            return self.public_key.verify_signature(hs, signature.clone());
        }
        false
    }
}

pub struct ParsedConfig {
    pub known_addresses : Vec<ParsedAddress>, // do we need this?
    pub proposal : Option<String>,
    pub private_key : PrivateKey,
    pub address : ParsedAddress,
    pub learners : HashMap<String, Vec<HashSet<ParsedAddress>>>,
}


pub fn from_json(s: &str) -> serde_json::error::Result<ParsedConfig> {
    let Config { private_key : private_key_string,
                 proposal : proposal_string,
                 learners : grpc_learners,
                 addresses : grpc_addresses } = serde_json::from_str(s)?;
    let known_addresses : Vec<ParsedAddress> = grpc_addresses.into_iter().map(
        |Address{name, hostname, port, public_key}| 
            ParsedAddress {
                name,
                hostname,
                port,
                public_key : PublicKey::new_default_scheme(public_key),
            }).collect();
    let addresses_by_name : HashMap<String, ParsedAddress> = known_addresses.iter().map(
        |address| (address.name.clone(), address.clone())).collect();
    let private_key = PrivateKey::new_default_scheme(private_key_string);
    let signature = private_key.sign_bytes(&vec![1,2,3,4,5][..]);
    let address = known_addresses.iter().find(
        |p| p.public_key.verify_bytes(&vec![1,2,3,4,5][..], signature.clone())
        ).expect("my private key did not match any known public key").clone();
    let get_address = |name : &String|
        addresses_by_name.get(name).expect(&format!("name {} not found in addresses", name)).clone();
    let learners = grpc_learners.iter().map(
        |(learner_name, mqs)|
          (learner_name.to_string(),
           mqs.quorums.iter().map(|q| q.names.iter().map(get_address).collect()).collect())).collect();
    let proposal = if proposal_string.len() > 0 {Some(proposal_string)} else {None};
    Ok(ParsedConfig{
        known_addresses,
        proposal,
        private_key,
        address,
        learners,
       })
}

