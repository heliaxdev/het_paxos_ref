use het_paxos_ref::{config::{Address,
                             Config,
                             Quorum,
                             MinimalQuorums,
                             SafetyEdges},
                    crypto::new_key_pair};
use serde_json::to_writer_pretty;
use std::{collections::HashMap,
          env::args,
          error::Error,
          fs::OpenOptions};


/// Generate a basic config file featuring 4 homogeneous acceptors, 1 learner, and a proposal.
/// First arg will be used as the prefix of the output .json config file names.
fn main() -> Result<(), Box<dyn Error>>{
    let alice = "Alice".to_string();
    let bob = "Bob".to_string();
    let carol = "Carol".to_string();
    let dave = "Dave".to_string();
    let eve = "Eve".to_string();
    let addresses : Vec<_> = [&alice,&bob,&carol,&dave].iter().enumerate().map(
        |(i, name)| {
            let (public_key, private_key) = new_key_pair(&["localhost".to_string()]);
            (private_key, 
             Address{
                public_key : public_key.to_string(),
                hostname : "localhost".to_string(),
                port : 8700 + i as u32,
                name : name.to_string()
             })
        }).collect();
    for (i, (private_key, _)) in addresses.iter().enumerate() {
        to_writer_pretty(
            OpenOptions::new().write(true).create(true).open(
                format!("{}_{}_config.json", args().collect::<Vec<_>>().get(1)
                    .expect("filename argument required"), i))?,
            &Config{
                private_key : private_key.to_string(),
                proposal : format!("my_proposal_{}", i),
                learners : HashMap::from([
                    (eve.clone() , MinimalQuorums{quorums : vec![
                        Quorum{names : vec![alice.clone(), bob.clone(), carol.clone()]},
                        Quorum{names : vec![alice.clone(), bob.clone(), dave.clone()]},
                        Quorum{names : vec![alice.clone(), carol.clone(), dave.clone()]},
                        Quorum{names : vec![bob.clone(), carol.clone(), dave.clone()]},
                    ]}),
                ]),
                safety_sets : HashMap::from([
                  (eve.clone(),
                    SafetyEdges{safety_sets : HashMap::from([
                      (eve.clone(), MinimalQuorums{quorums : vec![
                        Quorum{names : vec![alice.clone(), bob.clone(), carol.clone()]},
                        Quorum{names : vec![alice.clone(), bob.clone(), dave.clone()]},
                        Quorum{names : vec![alice.clone(), carol.clone(), dave.clone()]},
                        Quorum{names : vec![bob.clone(), carol.clone(), dave.clone()]},
                        ]})
                    ])})
                ]),
                addresses : addresses.iter().map(|(_,a)| a.clone()).collect()
            })?;
    }
    Ok(())
}
