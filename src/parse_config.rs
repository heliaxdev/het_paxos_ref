use crate::{config::{Address, Config},
            crypto::{PublicKey, PrivateKey},
           };
use serde_json;
use std::collections::{HashMap, HashSet};

// deliberately private: you should not be making these outside this module.
// I'd like to use something more clever.
// For now, we're using index in the vector of known addresses.
type AddressIndex = usize;

pub struct ParsedAddress {
    pub name : String,
    pub hostname : String,
    pub port : u32,
    pub public_key : PublicKey,
    index : AddressIndex,
}

impl ParsedAddress {
    pub fn get_index(&self) -> AddressIndex {
        self.index
    }
}

pub struct ParsedConfig {
    pub known_addresses : Vec<ParsedAddress>,
    pub proposal : Option<String>,
    pub private_key : PrivateKey,
    pub address : AddressIndex, // points to a public key in known_public_keys
    pub learners : HashMap<AddressIndex, Vec<HashSet<AddressIndex>>>,
}

impl ParsedConfig {
    pub fn get_address(&self, index : AddressIndex) -> &ParsedAddress {
        self.known_addresses.get(index).expect(
            &format!("index {} not found in known_addresses", index))
    }
}

pub fn from_json(s: &str) -> serde_json::error::Result<ParsedConfig> {
    let Config { private_key : private_key_string,
                 proposal : proposal_string,
                 learners : grpc_learners,
                 addresses : grpc_addresses } = serde_json::from_str(s)?;
    let known_addresses : Vec<ParsedAddress> = grpc_addresses.into_iter().enumerate().map(
        |(index, Address{name, hostname, port, public_key})| 
            ParsedAddress {
                name,
                hostname,
                port,
                public_key : PublicKey::new_default_scheme(public_key),
                index,
            }).collect();
    let addresses_by_name : HashMap<String, AddressIndex> = known_addresses.iter().map(
        |address| (address.name.clone(), address.get_index())).collect();
    let private_key = PrivateKey::new_default_scheme(private_key_string);
    let signature = private_key.sign_bytes(&vec![1,2,3,4,5][..]);
    let address = known_addresses.iter().find(
        |p| p.public_key.verify_bytes(&vec![1,2,3,4,5][..], signature.clone())
        ).expect("my private key did not match any known public key").get_index();
    let get_index = |name : &String|
        addresses_by_name.get(name).expect(&format!("name {} not found in addresses", name)).clone();
    let learners = grpc_learners.iter().map(
        |(learner_name, mqs)|
          (get_index(learner_name),
           mqs.quorums.iter().map(|q| q.names.iter().map(get_index).collect()).collect())).collect();
    let proposal = if proposal_string.len() > 0 {Some(proposal_string)} else {None};
    Ok(ParsedConfig{
        known_addresses,
        proposal,
        private_key,
        address,
        learners,
       })
}

