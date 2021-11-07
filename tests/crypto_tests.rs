use het_paxos_ref::config::Address;
use het_paxos_ref::crypto::new_key_pair;

#[test]
fn message_sign_verify() {
    let address = Address {
        public_key : "ABCDEFG".to_string(),
        hostname : "example.gov".to_string(),
        port : 1337,
        name : "alice".to_string()
    };
    let (public_key, private_key) = new_key_pair(&["example.gov".to_string()]);
    let signature = private_key.sign_message(&address);
    assert!(public_key.verify_signature(&address, signature));
}
