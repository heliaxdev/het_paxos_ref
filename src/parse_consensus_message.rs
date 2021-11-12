use crate::{config::{Address, Config},
            crypto::{PublicKey, PrivateKey},
            grpc::{Ballot, ConsensusMessage},
            parse_config::{ParsedAddress, ParsedConfig},
           };
use serde_json;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PrecomputedConsensusMessageStuff {
    pub original : ConsensusMessage,
    pub hash : Hash256,
    pub transitive_references_except_self : HashSet<ParsedConsensusMessage>,
    pub caught : HashSet<ParsedAddress>,
    pub connected : HashMap<ParsedAddress, HashSet<ParsedAddress>>,
    pub buried : HashMap<TwoA, HashSet<ParsedAddress>>,
    pub connected_2as : HashMap<ParsedAddress, HashSet<TwoA>>,
}



#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OneA {
    ballot : Ballot,
    precomputed : PrecomputedConsensusMessageStuff,
}


#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OneB {
    sig : ParsedAddress,
    my_1a : OneA,
    fresh : HashSet<ParsedAddress>,
    precomputed : PrecomputedConsensusMessageStuff,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TwoA {
    sig : ParsedAddress,
    my_1a : OneA,
    quorum : HashSet<OneB>,
    precomputed : PrecomputedConsensusMessageStuff,
}



#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ParsedConsensusMessage {
    NotWellFormed(PrecomputedConsensusMessageStuff),
    OneA(OneA),
    OneB(OneB),
    TwoA(TwoA),
}

pub trait HasPrecomputedConsensusMessageStuff {
    fn get_precomputed(&self) -> &PrecomputedConsensusMessageStuff;
}

impl<T: HasPrecomputedConsensusMessageStuff> Hash for T {
    fn hash<H: Hasher>(&self, state : &mut H) {
        self.get_precomputed().hash.hash(state)
    }
}

impl<T: HasPrecomputedConsensusMessageStuff> PartialEq for T {
   fn eq(&self, other: &Self) -> bool {
        self.get_precomputed().hash == other.get_precomputed().hash
    }
}

impl<T: HasPrecomputedConsensusMessageStuff> Eq for T {}

impl<T: HasPrecomputedConsensusMessageStuff> PartialOrd for T 
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_precomputed().hash.partial_cmp(other.get_precomputed().hash)
    }
}


impl PrecomputedConsensusMessageStuff for HasPrecomputedConsensusMessageStuff {
    fn get_precomputed(&self) -> &PrecomputedConsensusMessageStuff {self}
}

impl OneA for HasPrecomputedConsensusMessageStuff {
    fn get_precomputed(&self) -> &PrecomputedConsensusMessageStuff {self.precomputed}
}

impl OneB for HasPrecomputedConsensusMessageStuff {
    fn get_precomputed(&self) -> &PrecomputedConsensusMessageStuff {self.precomputed}
}

impl TwoA for HasPrecomputedConsensusMessageStuff {
    fn get_precomputed(&self) -> &PrecomputedConsensusMessageStuff {self.precomputed}
}

impl ParsedConsensusMessage for HasPrecomputedConsensusMessageStuff {
    fn get_precomputed(&self) {
        match self {
            NotWellFormed(x) => x.get_precomputed();
            OneA(x) => x.get_precomputed();
            OneB(x) => x.get_precomputed();
            TwoA(x) => x.get_precomputed();
        }
    }
}

pub trait HasSig {
    fn get_sig(&self) -> &ParsedAddress;
}

impl HasSig for OneB {
    fn get_sig(&self) -> &ParsedAddress {
        &self.sig
    }
}

impl HasSig for TwoA {
    fn get_sig(&self) -> &ParsedAddress {
        &self.sig
    }
}

pub trait HasOneA {
    fn get_1a(&self) -> &OneA;

    fn ballot(&self) -> &Ballot {
        self.get_1a().ballot
    }

    fn value(&self) -> &Hash256 {
        self.ballot().value_hash.expect("this OneA has no value_hash in its ballot!")
    }
}

impl HasOneA for OneA{
    fn get_1a(&self) -> &OneA {self}
}
impl HasOneA for OneB{
    fn get_1a(&self) -> &OneA {&self.my_1a}
}
impl HasOneA for TwoA{
    fn get_1a(&self) -> &OneA {&self.my_1a}
}

