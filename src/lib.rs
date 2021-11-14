pub mod acceptor;
pub mod crypto;
pub mod parse_config;

pub mod grpc {
    tonic::include_proto!("hetpaxosref");
    include!(concat!(env!("OUT_DIR"), "/hetpaxosref.serde.rs"));

    impl ConsensusMessage {
        /// quick check to see if this is a one_a message: does it have a ballot with a timestamp
        /// and hash_value?
        pub fn is_one_a(&self) -> bool {
            match self {
                ConsensusMessage{message_oneof : Some(consensus_message::MessageOneof::Ballot(
                        Ballot{timestamp : Some(_), value_hash : Some(_)}))} => true,
                _ => false,
            }
        }
    }
}

pub mod config {
    tonic::include_proto!("hetpaxosrefconfig");
    include!(concat!(env!("OUT_DIR"), "/hetpaxosrefconfig.serde.rs"));
}

pub mod utils {
    use byteorder::{BigEndian, ByteOrder};
    use crate::grpc::{Ballot, Hash256};
    use pbjson_types::Timestamp;
    use prost:: Message;
    use sha3::{Digest, Sha3_256};
    use std::{hash::{Hash, Hasher}, cmp::Ordering};

    /// Hash a protobuf Message struct with Sha3 into a Hash256 struct.
    /// Bytes marshaled in BigEndian order.
    pub fn hash(message : &impl Message) -> Hash256 {
        let bytes = Sha3_256::digest(&message.encode_to_vec()[..]);
        Hash256 {
            bytes0_through7   : BigEndian::read_u64(&bytes[0..=7]),
            bytes8_through15  : BigEndian::read_u64(&bytes[8..=15]),
            bytes16_through23 : BigEndian::read_u64(&bytes[16..=23]),
            bytes24_through31 : BigEndian::read_u64(&bytes[24..=31]),
        }
    }

    /// Ironically, we want to make Hash256 objects hashable
    impl Hash for Hash256 {
        fn hash<H: Hasher>(&self, state : &mut H) {
            self.bytes0_through7.hash(state);
            self.bytes8_through15.hash(state);
            self.bytes16_through23.hash(state);
            self.bytes24_through31.hash(state);
        }
    }

    impl Eq for Hash256 {}

    /// We want to be able to compare hashes using < etc.
    impl Ord for Hash256 {
        fn cmp(&self, other: &Self) -> Ordering {
            (self.bytes0_through7,
             self.bytes8_through15,
             self.bytes16_through23,
             self.bytes24_through31).cmp(
           &(other.bytes0_through7,
             other.bytes8_through15,
             other.bytes16_through23,
             other.bytes24_through31))
        }
    }

    impl PartialOrd for Hash256 {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Eq for Ballot {}

    /// We want to be able to compare ballots using < etc.
    impl Ord for Ballot {
        fn cmp(&self, other: &Self) -> Ordering {
            fn timestamp_tuple(timestamp : &Option<Timestamp>) -> (i64, i32) {
                if let Some(t) = timestamp {
                   (t.seconds, t.nanos)
                } else {
                   (0, 0)
                }
            }
            (timestamp_tuple(&self.timestamp), &self.value_hash).cmp(
             &(timestamp_tuple(&other.timestamp), &other.value_hash))
        }
    }

    impl PartialOrd for Ballot {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
