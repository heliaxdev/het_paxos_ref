use het_paxos_ref::{proposer::launch_proposer,
                    parse_config::from_json};
use std::{env::args,
          error::Error,
          io::{Read, stdin},
          fs::read_to_string};
use tokio;


/// Run a basic proposer
/// first command line arg should be filename of config file
/// OR you can pipe in the config json in stdin
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let args : Vec<String> = args().collect();
    launch_proposer(from_json(&
        if let Some(filename) = args.get(1) {
            read_to_string(filename)?
        } else {
            let mut buffer = "".to_string();
            stdin().read_to_string(&mut buffer)?;
            buffer
        }
    )?).await
}
