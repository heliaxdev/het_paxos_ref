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


/// Parsed form of a (well-formed) message.
/// Create a new one using a `ConsensusMessage` via `ParsedMessage::new`.
/// `ParsedMessage`s with the same hash _must_ be equal.
/// We're going to use hashes for comparison and equality and whatnot.
///
/// This uses a lot of `Arc`s so that cloning is constant time and compact.
/// There should not be reference cycles, as all `ConsensusMessage`s
///  reference by hash.
///
/// Learners are represented by `String`s from the `ParsedConfig`.
/// Acceptors are represented by `ParsedAddress`s from the `ParsedConfig`.
#[derive(Debug)]
pub struct ParsedMessage {
    /// Original (protobuf) `ConsensusMessage` from which this is formed.
    my_original : ConsensusMessage,
    /// Hash of the original (protobuf) `ConsensusMessage` from which this is formed.
    /// (uniquely identifies this `ParsedMessage`.)
    my_hash : Hash256,
    /// Parsed forms of all the `ConsensusMessage`s in the
    ///  "causal past" of this message.
    /// In paticular, includes parsed forms of the messages this
    ///  `ConsensusMessage` referred to via hash, as well as all their
    ///  `transitive_references_excluding_self`.
    /// Similar to `Tran(x)` in
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    /// Does not include a ref to this `ParsedMessage`, as that would
    ///  create a reference cycle. 
    transitive_references_excluding_self : HashSet<Arc<ParsedMessage>>,
    /// If this message is of type 1A, then `None`.
    /// Otherwise, references the highest `Ballot` message of type 1A
    ///  in `transitive_references_excluding_self`.
    /// Represents `Get1a` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253)
    my_one_a : Option<Arc<ParsedMessage>>,
    /// Reference to the actor who signed this `ConsensusMessage`, or
    ///  `None`, if it is not a `SignedHashSet`.
    /// Similar to `Sig(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    my_signer : Option<Arc<ParsedAddress>>,
    /// References to all the `Acceptor`s which are proven Byzantine
    ///  in this message (and its
    ///  `transitive_references_excluding_self`)
    /// Similar to `Caught(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    my_caught : HashSet<Arc<ParsedAddress>>,
    /// For each Learner, the set of Learners to which it is still
    ///  connected (as of this message).
    /// Similar to `Con_a(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    my_connected_learners : HashMap<Arc<String>, HashSet<Arc<String>>>,
    /// For each Learner, the set of 2A messages known to be buried as
    ///  of this message.
    /// Similar to `Buried(x,y)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    my_buried_two_as : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>,
    /// If this is a 2A message, then for each learner, the set of
    ///  other 2A messages connected to this message.
    /// Similar to `Con2as_a(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    my_connected_two_as : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>,
    /// Is this message of type 1B?
    i_am_one_b : bool,
    /// If this is a 1B, then for which Learners is it fresh?
    /// (See [the Technical Report](https://arxiv.org/abs/2011.08253)
    ///  for `fresh`.)
    i_am_fresh : HashSet<Arc<String>>,
    /// If this is a 2A, then for each learner, what quorum of 1B
    ///  messages is it formed from?
    /// Similar to `q(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    my_quorum : HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>,
    /// For which Learners is this message a 2A?
    /// (If this is not a 2A, this should be empty.)
    my_two_a_learners : HashSet<Arc<String>>,
    /// Convenience field: sometimes it's nice to be able to return an
    ///  empty set of learners, so rather than creating one every
    ///  time, we just keep this around. 
    /// Maybe it should be static or something?
    empty_learners : HashSet<Arc<String>>,
    /// Convenience field: sometimes it's nice to be able to return an
    ///  empty set of messages, so rather than creating one every
    ///  time, we just keep this around. 
    /// Maybe it should be static or something?
    empty_messages : HashSet<Arc<ParsedMessage>>,
}

// Define some basically useful properties:
// mostly we'll just use the hash as a proxy for equality, ordering, etc.

/// ParsedMessages can be compared using ==.
impl PartialEq for ParsedMessage {
    /// Equality checks only use `self.get_hash()`.
    /// `ParsedMessage`s with the same hash literally _must_ be equal.
    fn eq(&self, other : &Self) -> bool {
        self.my_hash == other.my_hash
    }
}

/// ParsedMessages can be compared using ==.
/// I'm pretty sure this inherits implementation from PartialEq.
impl Eq for ParsedMessage {}

/// ParsedMessages can be compared using < etc.
impl Ord for ParsedMessage {
    /// We just use `self.get_hash()` for comparison.
    fn cmp(&self, other : &Self) -> Ordering {
        self.my_hash.cmp(&other.my_hash)
    }
}

/// ParsedMessages can be compared using < etc.
impl PartialOrd for ParsedMessage {
    /// We just use `self.get_hash()` for comparison.
    /// Uses the implementation from `Ord`.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Parsed Messages can be displayed, by default, as their hash.
impl Display for ParsedMessage {
    /// Displays as "ParsedMessage{{`self.get_hash() printed here`}}"
    fn fmt(&self, f : &mut Formatter<'_>) -> Result {
        write!(f, "ParsedMessage{{{}}}", self.my_hash)
    }
}

/// We hash a `ParsedMessage` by just using its `self.get_hash()`
impl Hash for ParsedMessage {
    /// We hash a `ParsedMessage` by just using its `self.get_hash()`
    /// Should therefore be constant time.
    fn hash<H: Hasher>(&self, state : &mut H) {
        self.my_hash.hash(state)
    }
}

/// A trait for references that refer to a `ParsedMessage`.
/// We will use these pretty much everywhere we want to refer to a
///  `ParsedMessage`.
/// Includes useful accessor functions.
/// Pretty much the only implementation of this is for
///  `Arc<ParsedMessage>`, but in principle you could have others.
pub trait ParsedMessageRef {
    /// Hash of the original (protobuf) `ConsensusMessage` from which this is formed.
    /// (uniquely identifies this `ParsedMessage`.)
    fn get_hash<'a>(&'a self) -> &'a Hash256;
    /// Parsed forms of all the `ConsensusMessage`s in the
    ///  "causal past" of this message.
    /// In paticular, includes parsed forms of the messages this
    ///  `ConsensusMessage` referred to via hash, as well as all their
    ///  `transitive_references()`.
    /// Similar to `Tran(x)` in
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    /// Has to use `self` so that the returned set can include `self`.
    fn transitive_references(&self) -> HashSet<Self> where Self:Sized;
    /// Original (protobuf) `ConsensusMessage` from which this is formed.
    fn original<'a>(&'a self) -> &'a ConsensusMessage;
    /// If this message is of type 1A, then `None`.
    /// Otherwise, references the highest `Ballot` message of type 1A
    ///  in `self.transitive_references()`.
    /// Represents `Get1a` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253)
    fn one_a<'a>(&'a self) -> &'a Self;
    /// Is this message of type 1A?
    fn is_one_a(&self) -> bool;
    /// Reference to the actor who signed this `ConsensusMessage`, or
    ///  `None`, if it is not a `SignedHashSet`.
    /// Similar to `Sig(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    fn signer<'a>(&'a self) -> &'a Option<Arc<ParsedAddress>>;
    /// References to all the `Acceptor`s which are proven Byzantine
    ///  in `self` (and `self.transitive_references()`)
    /// Similar to `Caught(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    fn caught<'a>(&'a self) -> &'a HashSet<Arc<ParsedAddress>>;
    /// The set of Learners to which the input Learner is still
    ///  connected (as of this message).
    /// Similar to `Con_a(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    fn connected(&self, learner : &Arc<String>) -> &HashSet<Arc<String>>;
    /// Given a Learner, the set of 2A messages known to be buried as
    ///  of this message.
    /// Similar to `Buried(x,y)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    fn buried(&self, learner : &Arc<String>) -> &HashSet<Self> where Self:Sized;
    /// Is this message of type 1B?
    fn is_one_b(&self) -> bool;
    /// If this is a 2A message, then given a learner, the set of
    ///  other 2A messages connected to this message.
    /// Similar to `Con2as_a(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    fn connected_two_as(&self, learner : &Arc<String>) -> &HashSet<Self> where Self:Sized;
    /// Is this a fresh 1B for the given Learner?
    /// (See [the Technical Report](https://arxiv.org/abs/2011.08253)
    ///  for `fresh`.)
    fn fresh(&self, learner : &Arc<String>) -> bool;
    /// If this is a 2A for the given learner, what quorum of 1B
    ///  messages is it formed from?
    /// Similar to `q(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    fn quorum(&self, learner : &Arc<String>) -> &HashSet<Self> where Self:Sized;
    /// For which Learners is this message a 2A?
    /// (If this is not a 2A, this should be empty.)
    fn two_a_learners(&self) -> &HashSet<Arc<String>>;
    /// Returns the ballot of this message.
    /// Similar to `b(x)` in 
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    ///
    /// Default implementation uses `self.one_a()` and panics if that
    ///  returns a message with no `Ballot`.
    fn ballot<'a>(&'a self) -> &'a Ballot where Self : Debug {
        if let ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(b))} = self.one_a().original() {
            return b;
        }
        panic!("self.one_a() returned a message with no ballot: {:?}", self.one_a())
    }
    /// The hash of the value proposed in this message's 1A.
    ///
    /// Default implementation uses `self.one_a()` and panics if that
    ///  returns a message with no `value_hash`.
    fn value_hash<'a>(&'a self) -> &'a Hash256  where Self : Debug{
        &self.ballot().value_hash.as_ref().expect(&format!("ballot with no value hash: {:?}", self.one_a()))
    }
    /// Is this message of type 2A with the given Learner?
    /// See
    ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
    ///
    /// Default implementation uses `self.two_a_learners`.
    fn is_two_a_with_learner(&self, learner : &Arc<String>) -> bool {
        self.two_a_learners().contains(learner)
    }
    /// Is this message of type 2A for any Learner?
    ///
    /// Default implementation uses `self.two_a_learners`.
    fn is_two_a(&self) -> bool {
        !self.two_a_learners().is_empty()
    }
}

// Do I need to document these seperately, or is the trait documentation enough?
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
        match &self.my_one_a {
            Some(x) => &x,
            _ => &self
        }
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
        self.i_am_fresh.contains(learner)
    }
    fn quorum(&self, learner : &Arc<String>) -> &HashSet<Arc<ParsedMessage>> {
        self.my_quorum.get(learner).clone().unwrap_or(&self.empty_messages)
    }
    fn two_a_learners(&self) -> &HashSet<Arc<String>> {
        &self.my_two_a_learners
    }
}

impl ParsedMessage {
    /// Generates a new Parsed Message.
    /// Returns None when the message is not well-formed.
    /// - Requires a `ParsedConfig` to look up Learners and Acceptors.
    /// - Requires a function to look up known messages by hash.
    ///   This _must_ inclue any hashes referenced in `my_original`.
    /// - `my_hash` should be the hash of `my_original`.
    ///   In principle, we could re-calculate this, but we don't.
    ///   Maybe that should be a testable invariant somewhere?
    ///
    /// This function is where a lot of the heavy work takes place.
    pub fn new(my_original : ConsensusMessage,
               my_hash : Hash256,
               config : &ParsedConfig, 
               known_messages : &impl Fn(&Hash256) -> Option<Arc<ParsedMessage>>,
    ) -> Option<ParsedMessage> {

        /// Is this pair of learners connected?
        /// Specifically, given this set of `caught` Acceptors, does
        ///  this pair of learners have an uncaught acceptor in each
        ///  of their quorum intersections?
        fn connected_learners(config : &ParsedConfig,
                              learner_x : &Arc<String>,
                              learner_y : &Arc<String>,
                              caught : &HashSet<Arc<ParsedAddress>>)
                              -> bool {
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

        /// To which other Learners is `learner` connected, given that
        ///  `caught` Accetpors are Byzantine?
        fn connected<'a>(config : &ParsedConfig,
                         learner : &Arc<String>,
                         caught : &HashSet<Arc<ParsedAddress>>)
                         -> HashSet<Arc<String>> {
            config.learners.keys().filter(|x| connected_learners(config, learner, x, &caught)).map(|x| x.clone()).collect()
        }

        /// Maps all Learners to the set of other Learners to which
        ///  they are still connected, given that `caught` Acceptors
        ///  are Byzantine.
        fn connected_learners_map<'a>(config : &ParsedConfig,
                                      caught : &HashSet<Arc<ParsedAddress>>)
                                      -> HashMap<Arc<String>, HashSet<Arc<String>>> {
            config.learners.keys().map(|k| (k.clone(), connected(config, k, caught))).collect()
        }

        /// The set of all Acceptors who signed any message in
        ///  `messages`.
        /// Similar to `Sig(x)` in 
        ///  [the Technical Report](https://arxiv.org/abs/2011.08253).
        fn sigs<'a>(messages : impl Iterator<Item = &'a Arc<ParsedMessage>>) -> HashSet<Arc<ParsedAddress>> {
            messages.filter_map(|m| m.signer().as_ref()).map(|x| x.clone()).collect()
        }

        // If we're a 1a, things are relatively easy.
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
                i_am_fresh : HashSet::new(),
                my_quorum : HashMap::new(),
                my_two_a_learners : HashSet::new(),
                empty_learners : HashSet::new(),
                empty_messages : HashSet::new(),
            });
        }

        // If we're not a 1a, we should have some transitive references.
        // Here, `refs` is set to the list of reference hashes
        if let ConsensusMessage{message_oneof : Some(MessageOneof::SignedHashSet(
                   SignedHashSet{hash_set : Some(ref refs),..}))} = my_original {
            // the set of `ParsedMessage`s that this message references
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
                            |x,y| x.union(y.caught()).map(|x| x.clone()).collect());
            let my_connected_learners = connected_learners_map(config, &my_caught);
            let my_buried_two_as = config.learners.iter().map(|(learner, quorums)| (learner.clone(),
                    transitive_references_excluding_self.iter()
                    .filter(|x| x.is_two_a_with_learner(learner)) // filter for 2As
                    .filter(|x| { // see `Buried` from [the Technical Report](https://arxiv.org/abs/2011.08253).
                        let s = sigs(transitive_references_excluding_self.iter().filter(|m|
                                             m.transitive_references().contains(*x) 
                                          && m.transitive_references().iter().any(|z|
                                                  z.is_two_a_with_learner(learner)
                                               && z.value_hash() != x.value_hash()
                                               && z.ballot() > x.ballot())));
                         quorums.iter().any(|quorum| quorum.iter().all(|a| s.contains(a)))
                    }).map(|x| x.clone()).collect::<HashSet<Arc<ParsedMessage>>>()
                )).collect::<HashMap<Arc<String>, HashSet<Arc<ParsedMessage>>>>();
            let my_one_a = transitive_references_excluding_self.iter() // find max 1A in trans. references
                             .filter(|x| x.is_one_a())
                             .map(|x| x.clone())
                             .max_by_key(|x| x.ballot().clone());
            if let Some(ref one_a) = my_one_a { // Not well-formed unless we actually have a 1A. 
                let i_am_one_b = ref_messages.iter().contains(&one_a); // Only 1B messages directly reference 1As.
                if let ConsensusMessage{message_oneof : Some(MessageOneof::Ballot(ballot))} = one_a.original() {
                    // Not well-formed unless we actually have a `ballot`.
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
                    if i_am_one_b || !my_two_a_learners.is_empty() { // to be well-formed, this must be a 1b or a 2a
                        for my_address in &config.known_addresses { // really dumb way to find out who signed this
                            // Optimization opportunity: 
                            //  There MUST be a better way to find out
                            //   who signed this message than to
                            //   simply try all possibly signers. 
                            if my_address.signed(&my_original) {
                                let my_signer = Some(my_address.clone());
                                // Next we check if this message would
                                //  be a redundant 2A.
                                // If the signer has already made a 2A
                                //  that's "just as good," then we
                                //  consider this message to not be
                                //  well-formed.
                                // This is somewhat of a change from
                                //  [the Technical Report](https://arxiv.org/abs/2011.08253).
                                // Specifically, we check for a 2A in
                                //  transitive references that has:
                                // - the same ballot as this message
                                // - the same signer as this message
                                // - quorums for each learner that are
                                //   no smaller than they are in this
                                //   message
                                if !transitive_references_excluding_self.iter().any(|m|
                                        m.is_two_a()
                                     && m.signer() == &my_signer
                                     && m.ballot() == ballot
                                     && my_quorum.iter().all(|(learner, quorum)|
                                         m.quorum(learner).is_superset(quorum)
                                     )) { 
                                    if let Some(v) = &ballot.value_hash { // well-formed requires ballots have `value_hash`.
                                        // encodes `Con2as_a(x)` from
                                        //  [the Technical Report](https://arxiv.org/abs/2011.08253)
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
                                        // encodes `fresh(x)` from
                                        //  [the Technical Report](https://arxiv.org/abs/2011.08253)
                                        let i_am_fresh = my_connected_two_as.iter().filter_map(|(learner, two_as)|
                                          if i_am_one_b && two_as.iter().all(|m| m.value_hash() == v) {
                                              Some(learner.clone())
                                          } else {
                                              None
                                          }).collect();
                                        return Some(ParsedMessage {
                                            my_original,
                                            my_hash,
                                            transitive_references_excluding_self,
                                            my_one_a,
                                            my_signer,
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
                            } // message was not signed by my_address, so check more possible signers.
                        } // loop complete but no signer found. this is mal-formed
                    } // not a 1b or a 2a. mal-formed.
                } // no ballot found. this is mal-formed.
            } // somehow we didn't have a 1a here.
        }// we're not a 1a and we don't have any references
        None
    }
}
