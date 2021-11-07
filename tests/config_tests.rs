use het_paxos_ref::config::Address;
use prost::Message;
use serde_json::{from_str, to_string_pretty};

#[test]
fn address_encode_decode() {
    let address = Address {
        public_key : "ABCDEFG".to_string(),
        hostname : "example.gov".to_string(),
        port : 1337,
        name : "alice".to_string()
    };
    let mut buf = Vec::new();
    assert!(address.encode(&mut buf).is_ok());
    let decoded = Address::decode(&buf[..]);
    assert!(decoded.is_ok());
    let decoded = decoded.unwrap();
    assert_eq!(address, decoded);
    assert_eq!(address.public_key, decoded.public_key);
    assert_eq!(address.hostname, decoded.hostname);
    assert_eq!(address.port, decoded.port);
    assert_eq!(address.name, decoded.name);
}

#[test]
fn address_to_json_and_back() {
    let address = Address {
        public_key : "ABCDEFG".to_string(),
        hostname : "example.gov".to_string(),
        port : 1337,
        name : "alice".to_string()
    };
    let json = to_string_pretty(&address);
    assert!(json.is_ok());
    let json : String = json.unwrap();
    let decoded = from_str(&json);
    assert!(decoded.is_ok());
    let decoded : Address = decoded.unwrap();
    assert_eq!(address, decoded);
    assert_eq!(address.public_key, decoded.public_key);
    assert_eq!(address.hostname, decoded.hostname);
    assert_eq!(address.port, decoded.port);
    assert_eq!(address.name, decoded.name);
}
