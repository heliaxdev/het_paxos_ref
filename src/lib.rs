pub mod crypto;

pub mod grpc {
    tonic::include_proto!("hetpaxosref");
    include!(concat!(env!("OUT_DIR"), "/hetpaxosref.serde.rs"));
}

pub mod config {
    tonic::include_proto!("hetpaxosrefconfig");
    include!(concat!(env!("OUT_DIR"), "/hetpaxosrefconfig.serde.rs"));
}

pub mod utils {
    use byteorder::{BigEndian, ByteOrder};
    use crate::grpc::Hash256;
    use prost:: Message;
    use sha3::{Digest, Sha3_256};

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
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
