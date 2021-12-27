use crate::{grpc::{Ballot,
                   ConsensusMessage,
                   consensus_message::MessageOneof,
                   Hash256,
                   SignedHashSet},
            parse_config::{ParsedAddress,
                           ParsedConfig}};
use itertools::Itertools;
use std::{collections:: {HashMap, HashSet},
          cmp::Ordering,
          fmt::{Display, Formatter, Result},
          hash::{Hash, Hasher},
          iter::once,
         };


#[derive(Debug)]
pub struct ParsedMessage<'a> {
    my_original : &'a ConsensusMessage,
    my_hash : &'a Hash256,
    transitive_references_excluding_self : HashSet<&'a ParsedMessage<'a>>,
    my_one_a : Option<&'a ParsedMessage<'a>>,
    my_signer : Option<&'a ParsedAddress>,
    my_caught : HashSet<&'a ParsedAddress>,
    my_connected_learners : HashMap<&'a String, HashSet<&'a String>>,
    my_buried_two_as : HashMap<&'a String, HashSet<&'a ParsedMessage<'a>>>,
    my_connected_two_as : HashMap<&'a String, HashSet<&'a ParsedMessage<'a>>>,
    i_am_one_b : bool,
    i_am_fresh : HashMap<&'a String, bool>,
    my_quorum : HashMap<&'a String, HashSet<&'a ParsedMessage<'a>>>,
    my_two_a_learners : HashSet<&'a String>,
    empty_learners : HashSet<&'static String>,
    empty_messages : HashSet<&'a ParsedMessage<'a>>,
}

// Define some basically useful properties:
// mostly we'll just use the hash as a proxy for equality, ordering, etc.

impl<'a> PartialEq for ParsedMessage<'a> {
    fn eq(&self, other : &Self) -> bool {
        self.get_hash() == other.get_hash()
    }
}
impl<'a> Eq for ParsedMessage<'a> {}
impl<'a> Ord for ParsedMessage<'a> {
    fn cmp(&self, other : &Self) -> Ordering {
        self.get_hash().cmp(other.get_hash())
    }
}
impl<'a> PartialOrd for ParsedMessage<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> Display for ParsedMessage<'a> {
    fn fmt(&self, f : &mut Formatter<'_>) -> Result {
        write!(f, "ParsedMessage{{{}}}", self.get_hash())
    }
}
impl<'a> Hash for ParsedMessage<'a> {
    fn hash<H: Hasher>(&self, state : &mut H) {
        self.get_hash().hash(state)
    }
}

/// Now the real work begins
impl<'a> ParsedMessage<'a> {
    // accessors useful once this is assembled:
    pub fn get_hash(&self) -> &'a Hash256 {
        self.my_hash
    }
    pub fn transitive_references(&'a self) -> HashSet<&'a ParsedMessage<'a>> {
        once(self).chain(self.transitive_references_excluding_self.clone().into_iter()).collect()
    }
    pub fn original(&self) -> &'a ConsensusMessage {
        self.my_original
    }
    pub fn one_a(&'a self) -> &'a ParsedMessage<'a> {
        self.my_one_a.unwrap_or(self)
    }
    pub fn is_one_a(&self) -> bool {
        self.my_one_a.is_none()
    }
    pub fn ballot(&'a self) -> &'a Ballot {
        if let ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))} = self.one_a().original() {
            return b;
        }
        panic!("self.one_a() returned a message with no ballot: {:?}", self.one_a())
    }
    pub fn value_hash(&'a self) -> &'a Hash256 {
        &self.ballot().value_hash.as_ref().expect(&format!("ballot with no value hash: {:?}", self.one_a()))
    }
    pub fn signer(&self) -> &Option<&'a ParsedAddress> {
        &self.my_signer
    }
    pub fn caught(&self) -> &HashSet<&'a ParsedAddress> {
        &self.my_caught
    }
    pub fn connected(&self, learner : &'a String) -> &HashSet<&'a String> {
        self.my_connected_learners.get(learner).unwrap_or(&self.empty_learners)
    }
    pub fn buried(&self, learner : &'a String) -> &HashSet<&'a ParsedMessage<'a>> {
        self.my_buried_two_as.get(learner).unwrap_or(&self.empty_messages)
    }
    pub fn connected_two_as(&self, learner : &'a String) -> &HashSet<&'a ParsedMessage<'a>> {
        self.my_connected_two_as.get(learner).unwrap_or(&self.empty_messages)
    }
    pub fn is_one_b(&self) -> bool {
        self.i_am_one_b
    }
    pub fn fresh(&self, learner : &'a String) -> bool {
        *self.i_am_fresh.get(learner).unwrap_or(&false)
    }
    pub fn quorum(&self, learner : &'a String) -> &HashSet<&'a ParsedMessage<'a>> {
        self.my_quorum.get(learner).clone().unwrap_or(&self.empty_messages)
    }
    pub fn two_a_learners(&self) -> &HashSet<&'a String> {
        &self.my_two_a_learners
    }
    pub fn is_two_a_with_learner(&self, learner : &'a String) -> bool {
        self.two_a_learners().contains(learner)
    }
    pub fn is_two_a(&self) -> bool {
        !self.two_a_learners().is_empty()
    }


    // TODO: debug setting all those fields...
    // return None when the message is not well-formed.
    pub fn new(my_original : &'a ConsensusMessage,
               my_hash : &'a Hash256,
               config : &'a ParsedConfig, 
               known_messages : &impl Fn(&'a Hash256) -> Option<&'a ParsedMessage<'a>>,
    ) -> Option<ParsedMessage<'a>> {

        fn connected_learners(config : &ParsedConfig,
                              learner_x : &String,
                              learner_y : &String,
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
        fn connected<'a>(config : &'a ParsedConfig,
                         learner : &String,
                         caught : &HashSet<&ParsedAddress>)
                         -> HashSet<&'a String> {
            config.learners.keys().filter(|x| connected_learners(config, learner, x, &caught)).collect()
        }

        fn connected_learners_map<'a>(config : &'a ParsedConfig,
                                      caught : &HashSet<&ParsedAddress>)
                                      -> HashMap<&'a String, HashSet<&'a String>> {
            config.learners.keys().map(|k| (k, connected(config, k, caught))).collect()
        }

        fn sigs<'a>(messages : impl Iterator<Item = &'a &'a ParsedMessage<'a>>) -> HashSet<&'a ParsedAddress> {
            messages.filter_map(|m| m.signer().as_ref()).map(|x| *x).collect()
        }

        // if we're a 1a, things are relatively easy
        if my_original.is_one_a() {
            return Some(ParsedMessage {
                my_original,
                my_hash,
                transitive_references_excluding_self : HashSet::new(),
                my_one_a : None,
                my_signer : None,
                my_caught : HashSet::new(),
                my_connected_learners : connected_learners_map(config, &HashSet::new()),
                my_buried_two_as : HashMap::new(),
                my_connected_two_as : HashMap::new(),
                i_am_one_b : false,
                i_am_fresh : HashMap::new(),
                my_quorum : HashMap::new(),
                my_two_a_learners : HashSet::new(),
                empty_learners : HashSet::new(),
                empty_messages : HashSet::new(),
            });
        }
        // if we're not a 1a, we should realy have some transitive references:
        if let ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                   SignedHashSet{hash_set : Some(refs),..}))} = my_original {
            let ref_messages : HashSet<_> = refs.hashes.iter().filter_map(known_messages).collect();
            let transitive_references_excluding_self : HashSet<&'a ParsedMessage<'a>> = ref_messages.iter()
                    .fold(HashSet::new(), |x,y| x.union(&y.transitive_references()).map(|x| *x).collect());
            let my_caught = ref_messages.iter().fold(
                            // if any referenced message, with any transitively referenced message,
                            // catches anyone:
                            ref_messages.iter().cartesian_product(transitive_references_excluding_self.iter())
                                .filter_map(|(x,y)|
                                    if    x.signer().is_some()
                                       && x.signer() == y.signer()
                                       && !x.transitive_references().iter().contains(y)
                                       && !y.transitive_references().iter().contains(x) {
                                        x.signer().as_ref()
                                    } else {
                                        None
                                    }
                                ).map(|x| *x).collect::<HashSet<&'a ParsedAddress>>(),
                            // union everyone we've already caught:
                            |x,y| x.union(y.caught()).map(|x| *x).collect());
            let my_connected_learners = connected_learners_map(config, &my_caught);
            let my_buried_two_as = config.learners.iter().map(|(learner, quorums)| (learner,
                    transitive_references_excluding_self.iter()
                    .filter(|x| x.is_two_a_with_learner(learner))
                    .filter(|x| {
                        let s = sigs(transitive_references_excluding_self.iter().filter(|m|
                                             m.transitive_references().contains(*x) 
                                          && m.transitive_references().iter().any(|z|
                                                  z.is_two_a_with_learner(learner)
                                               && z.value_hash() != x.value_hash()
                                               && z.ballot() > x.ballot())));
                         quorums.iter().any(|quorum| quorum.iter().all(|a| s.contains(a)))
                    }).map(|x| *x).collect::<HashSet<&'a ParsedMessage<'a>>>()
                )).collect::<HashMap<&'a String, HashSet<&'a ParsedMessage<'a>>>>();
            let my_one_a = transitive_references_excluding_self.iter().map(|x| *x)
                             .filter(|x| x.is_one_a())
                             .max_by_key(|x| x.ballot());
            if let Some(one_a) = my_one_a {
                let i_am_one_b = ref_messages.iter().contains(&&one_a);
                if let ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(ballot))} = one_a.original() {
                    let my_quorum = config.learners.keys().map(|learner| (learner,
                            transitive_references_excluding_self.iter().filter(|m|
                                m.is_one_b()
                                && m.fresh(learner)
                                && m.ballot() == ballot
                            ).map(|x| *x).collect()
                        )).collect::<HashMap<&'a String, HashSet<&'a ParsedMessage<'a>>>>();
                    let my_two_a_learners = my_quorum.iter().filter_map(|(learner, quorum)| {
                            if let Some(quorums) = config.learners.get(*learner) {
                                let signers = sigs(quorum.iter());
                                if quorums.iter().any(|q| q.iter().all(|s| signers.contains(s))) {
                                    return Some(learner);
                                }
                            }
                            None
                        }).map(|x| *x).collect::<HashSet<&'a String>>();
                    if i_am_one_b || !my_two_a_learners.is_empty() { // if this is a 1b or a 2a
                        for my_address in &config.known_addresses {
                            if my_address.signed(my_original) {
                                let my_signer = Some(my_address);
                                if !transitive_references_excluding_self.iter().any(|m|
                                        m.is_two_a()
                                     && m.signer() == &my_signer
                                     && m.ballot() == ballot
                                     && my_quorum.iter().all(|(learner, quorum)|
                                         m.quorum(learner).is_superset(quorum)
                                     )) {
                                    if let Some(v) = &ballot.value_hash {
                                        let my_connected_two_as : HashMap<&'a String, HashSet<&'a ParsedMessage<'a>>> = if i_am_one_b { 
                                                my_connected_learners.iter().map(|(learner, connected)| (*learner, 
                                                    transitive_references_excluding_self.iter().filter(|m|
                                                        m.is_two_a()
                                                     && m.signer() == &my_signer
                                                     && m.two_a_learners().iter().any(|l| connected.contains(l))
                                                     && if let Some(buried) = my_buried_two_as.get(learner) {
                                                            !buried.contains(*m)
                                                        } else {
                                                            true
                                                        }
                                                ).map(|x| *x).collect())).collect()
                                            } else { HashMap::new() };
                                        let i_am_fresh = my_connected_two_as.iter().map(|(learner, two_as)|
                                                (*learner, i_am_one_b &&
                                                           two_as.iter().all(|m| m.value_hash() == v))).collect();
                                        return Some(ParsedMessage {
                                            my_original,
                                            my_hash,
                                            transitive_references_excluding_self,
                                            my_one_a,
                                            my_signer : Some(my_address),
                                            my_caught,
                                            my_connected_learners,
                                            my_buried_two_as,
                                            my_connected_two_as,
                                            i_am_one_b,
                                            i_am_fresh,
                                            my_quorum,
                                            my_two_a_learners,
                                            empty_learners : HashSet::new(),
                                            empty_messages : HashSet::new(),
                                        });
                                    } // ballot does not have a value_hash
                                } // sender has already sent a 2a that's as good or better.
                                return None; // message was correctly signed, but is not well-formed: cut the loop
                            } // message was not signed by my_address, so loop onward.
                        } // loop complete but no signer found. this is mal-formed
                    } // not a 1b or a 2a. mal-formed.
                } // no ballot found. this is mal-formed.
            } // somehow we didn't have a 1a here.
        }// we're not a 1a and we don't have any references
        None
    }
}
