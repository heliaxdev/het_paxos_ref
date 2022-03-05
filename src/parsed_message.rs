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
          fmt::{Debug, Display, Formatter, Result},
          hash::{Hash, Hasher},
          iter::once,
          sync::Arc,
         };


#[derive(Debug)]
pub struct ParsedMessage {
    my_original : ConsensusMessage,
    my_hash : Hash256,
    transitive_references_excluding_self : HashSet<Arc<ParsedMessage>>,
    my_one_a : Option<Arc<ParsedMessage>>,
    my_signer : Option<Arc<ParsedAddress>>,
    my_caught : HashSet<Arc<ParsedAddress>>,
    my_connected_learners : HashMap<Arc<String>, HashSet<Arc<String>>>,
    my_buried_two_as : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>,
    my_connected_two_as : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>,
    i_am_one_b : bool,
    i_am_fresh : HashMap<Arc<String>, bool>,
    my_quorum : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>,
    my_two_a_learners : HashSet<Arc<String>>,
    empty_learners : HashSet<Arc<String>>,
    empty_messages : HashSet<Arc<ParsedMessage>>,
}

// Define some basically useful properties:
// mostly we'll just use the hash as a proxy for equality, ordering, etc.

impl PartialEq for ParsedMessage {
    fn eq(&self, other : &Self) -> bool {
        self.my_hash == other.my_hash
    }
}
impl Eq for ParsedMessage {}
impl Ord for ParsedMessage {
    fn cmp(&self, other : &Self) -> Ordering {
        self.my_hash.cmp(&other.my_hash)
    }
}
impl PartialOrd for ParsedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Display for ParsedMessage {
    fn fmt(&self, f : &mut Formatter<'_>) -> Result {
        write!(f, "ParsedMessage{{{}}}", self.my_hash)
    }
}
impl Hash for ParsedMessage {
    fn hash<H: Hasher>(&self, state : &mut H) {
        self.my_hash.hash(state)
    }
}

pub trait ParsedMessageRef {
    // accessors useful once this is assembled:
    fn get_hash<'a>(&'a self) -> &'a Hash256;
    fn transitive_references(&self) -> HashSet<Self> where Self:Sized;
    fn original<'a>(&'a self) -> &'a ConsensusMessage;
    fn one_a<'a>(&'a self) -> &'a Self;
    fn is_one_a(&self) -> bool;
    fn signer<'a>(&'a self) -> &'a Option<Arc<ParsedAddress>>;
    fn caught<'a>(&'a self) -> &'a HashSet<Arc<ParsedAddress>>;
    fn connected(&self, learner : &Arc<String>) -> &HashSet<Arc<String>>;
    fn buried(&self, learner : &Arc<String>) -> &HashSet<Self> where Self:Sized;
    fn is_one_b(&self) -> bool;
    fn connected_two_as(&self, learner : &Arc<String>) -> &HashSet<Self> where Self:Sized;
    fn fresh(&self, learner : &Arc<String>) -> bool;
    fn quorum(&self, learner : &Arc<String>) -> &HashSet<Self> where Self:Sized;
    fn two_a_learners(&self) -> &HashSet<Arc<String>>;
    fn ballot<'a>(&'a self) -> &'a Ballot where Self : Debug {
        if let ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))} = self.one_a().original() {
            return b;
        }
        panic!("self.one_a() returned a message with no ballot: {:?}", self.one_a())
    }
    fn value_hash<'a>(&'a self) -> &'a Hash256  where Self : Debug{
        &self.ballot().value_hash.as_ref().expect(&format!("ballot with no value hash: {:?}", self.one_a()))
    }
    fn is_two_a_with_learner(&self, learner : &Arc<String>) -> bool {
        self.two_a_learners().contains(learner)
    }
    fn is_two_a(&self) -> bool {
        !self.two_a_learners().is_empty()
    }
}

/// Now the real work begins
impl ParsedMessageRef for Arc<ParsedMessage> {
    // accessors useful once this is assembled:
    fn get_hash<'a>(&'a self) -> &'a Hash256 {
        &self.my_hash
    }
    fn transitive_references(&self) -> HashSet<Arc<ParsedMessage>> {
        once(self.clone()).chain(self.transitive_references_excluding_self.clone().into_iter()).collect()
    }
    fn original<'a>(&'a self) -> &'a ConsensusMessage {
        &self.my_original
    }
    fn one_a<'a>(&'a self) -> &'a Arc<ParsedMessage> {
        &self.my_one_a.unwrap_or(*self)
    }
    fn is_one_a(&self) -> bool {
        self.my_one_a.is_none()
    }
    fn signer<'a>(&'a self) -> &'a Option<Arc<ParsedAddress>> {
        &self.my_signer
    }
    fn caught<'a>(&'a self) -> &'a HashSet<Arc<ParsedAddress>> {
        &self.my_caught
    }
    fn connected(&self, learner : &Arc<String>) -> &HashSet<Arc<String>> {
        self.my_connected_learners.get(learner).unwrap_or(&self.empty_learners)
    }
    fn buried(&self, learner : &Arc<String>) -> &HashSet<Arc<ParsedMessage>> {
        self.my_buried_two_as.get(learner).unwrap_or(&self.empty_messages)
    }
    fn connected_two_as(&self, learner : &Arc<String>) -> &HashSet<Arc<ParsedMessage>> {
        self.my_connected_two_as.get(learner).unwrap_or(&self.empty_messages)
    }
    fn is_one_b(&self) -> bool {
        self.i_am_one_b
    }
    fn fresh(&self, learner : &Arc<String>) -> bool {
        *self.i_am_fresh.get(learner).unwrap_or(&false)
    }
    fn quorum(&self, learner : &Arc<String>) -> &HashSet<Arc<ParsedMessage>> {
        self.my_quorum.get(learner).clone().unwrap_or(&self.empty_messages)
    }
    fn two_a_learners(&self) -> &HashSet<Arc<String>> {
        &self.my_two_a_learners
    }
}

impl ParsedMessage {
    // TODO: adjust everything to now use Arcs
    // return None when the message is not well-formed.
    pub fn new(my_original : ConsensusMessage,
               my_hash : Hash256,
               config : ParsedConfig, 
               known_messages : &impl Fn(&Hash256) -> Option<&Arc<ParsedMessage>>,
    ) -> Option<ParsedMessage> {

        fn connected_learners(config : &ParsedConfig,
                              learner_x : &Arc<String>,
                              learner_y : &Arc<String>,
                              caught : &HashSet<Arc<ParsedAddress>>)
                              -> bool {
            // does this pair of learners have an uncaught acceptor in each of their quorum
            // intersections?
            if let (Some(quorums_x), Some(quorums_y)) =
                   (config.learners.get(learner_x), config.learners.get(learner_y)) {
                for quorum_x in quorums_x {
                    for quorum_y in quorums_y {
                        if caught.is_superset(&quorum_x.intersection(quorum_y).map(|x| x.clone()).collect()) {
                            return false;
                        }
                    }
                }
                return (quorums_x.len() > 0) && (quorums_y.len() > 0);
            }
            false
        }
        fn connected<'a>(config : &ParsedConfig,
                         learner : &Arc<String>,
                         caught : &HashSet<Arc<ParsedAddress>>)
                         -> HashSet<Arc<String>> {
            config.learners.keys().filter(|x| connected_learners(config, learner, x, &caught)).map(|x| x.clone()).collect()
        }

        fn connected_learners_map<'a>(config : &ParsedConfig,
                                      caught : &HashSet<Arc<ParsedAddress>>)
                                      -> HashMap<Arc<String>, HashSet<Arc<String>>> {
            config.learners.keys().map(|k| (k.clone(), connected(config, k, caught))).collect()
        }

        fn sigs<'a>(messages : impl Iterator<Item = &'a Arc<ParsedMessage>>) -> HashSet<Arc<ParsedAddress>> {
            messages.filter_map(|m| m.signer().as_ref()).map(|x| x.clone()).collect()
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
                my_connected_learners : connected_learners_map(&config, &HashSet::new()),
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
            let transitive_references_excluding_self : HashSet<Arc<ParsedMessage>> = ref_messages.iter()
                    .fold(HashSet::new(), |x,y| x.union(&y.transitive_references()).map(|x| x.clone()).collect());
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
                                ).map(|x| x.clone()).collect::<HashSet<Arc<ParsedAddress>>>(),
                            // union everyone we've already caught:
                            |x,y| x.union(y.caught()).map(|x| *x).collect());
            let my_connected_learners = connected_learners_map(&config, &my_caught);
            let my_buried_two_as = config.learners.iter().map(|(learner, quorums)| (learner.clone(),
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
                    }).map(|x| x.clone()).collect::<HashSet<Arc<ParsedMessage>>>()
                )).collect::<HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>>();
            let my_one_a = transitive_references_excluding_self.iter().map(|x| *x)
                             .filter(|x| x.is_one_a())
                             .max_by_key(|x| x.ballot());
            if let Some(one_a) = my_one_a {
                let i_am_one_b = ref_messages.iter().contains(&&one_a);
                if let ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(ballot))} = one_a.original() {
                    let my_quorum = config.learners.keys().map(|learner| (learner.clone(),
                            transitive_references_excluding_self.iter().filter(|m|
                                m.is_one_b()
                                && m.fresh(learner)
                                && m.ballot() == ballot
                            ).map(|x| x.clone()).collect()
                        )).collect::<HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>>();
                    let my_two_a_learners = my_quorum.iter().filter_map(|(learner, quorum)| {
                            if let Some(quorums) = config.learners.get(learner) {
                                let signers = sigs(quorum.iter());
                                if quorums.iter().any(|q| q.iter().all(|s| signers.contains(s))) {
                                    return Some(learner);
                                }
                            }
                            None
                        }).map(|x| x.clone()).collect::<HashSet<Arc<String>>>();
                    if i_am_one_b || !my_two_a_learners.is_empty() { // if this is a 1b or a 2a
                        for my_address in &config.known_addresses {
                            if my_address.signed(&my_original) {
                                let my_signer = Some(my_address.clone());
                                if !transitive_references_excluding_self.iter().any(|m|
                                        m.is_two_a()
                                     && m.signer() == &my_signer
                                     && m.ballot() == ballot
                                     && my_quorum.iter().all(|(learner, quorum)|
                                         m.quorum(learner).is_superset(quorum)
                                     )) {
                                    if let Some(v) = &ballot.value_hash {
                                        let my_connected_two_as : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>> = if i_am_one_b { 
                                                my_connected_learners.iter().map(|(learner, connected)| (learner.clone(), 
                                                    transitive_references_excluding_self.iter().filter(|m|
                                                        m.is_two_a()
                                                     && m.signer() == &my_signer
                                                     && m.two_a_learners().iter().any(|l| connected.contains(l))
                                                     && if let Some(buried) = my_buried_two_as.get(learner) {
                                                            !buried.contains(*m)
                                                        } else {
                                                            true
                                                        }
                                                ).map(|x| x.clone()).collect())).collect()
                                            } else { HashMap::new() };
                                        let i_am_fresh = my_connected_two_as.iter().map(|(learner, two_as)|
                                                (learner.clone(), i_am_one_b && two_as.iter().all(|m| m.value_hash() == v))).collect();
                                        return Some(ParsedMessage {
                                            my_original,
                                            my_hash,
                                            transitive_references_excluding_self,
                                            my_one_a,
                                            my_signer : Some(*my_address),
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
