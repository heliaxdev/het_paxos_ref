# [Heterogeneous Paxos](https://arxiv.org/abs/2011.08253) Reference [Rust](https://www.rust-lang.org/) Implementation

## What This Is
This is intended to be a [reference implementation](https://en.wikipedia.org/wiki/Reference_implementation) for [Heterogeneous Paxos](https://arxiv.org/abs/2011.08253) ([OPODIS 2020](https://drops.dagstuhl.de/opus/volltexte/2021/13490/)), written in [Rust](https://www.rust-lang.org/).
While it is not the implementation used in the original publication or [the Technical Report](https://arxiv.org/abs/2011.08253), it is intended to provide a clean and correct implementation of Heterogeneous Paxos as specified in [the Technical Report](https://arxiv.org/abs/2011.08253), and is written by the same author, [Isaac Sheff](https://IsaacSheff.com).
Network message contents are kept to a bare minimum.
It is written as purely as possible in [Rust](https://www.rust-lang.org/), using [rustls](https://docs.rs/rustls/latest/rustls/) for cryptographic signatures, and the [tonic](https://docs.rs/tonic/latest/tonic/) [gRPC](https://grpc.io/) library for network communication.

## What This Is Not
- This implementation is _not_ optimized for performance or efficiency.
  It should not be used for any industrial purpose. 
  Several comments throughout the code point out places where optimizations should be possible.
- It is not [multi-paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)#Multi-Paxos).
  This implementation decides on one value.
  There are no "heights" or "slots."
- Timing protocols to guarantee termination in a [partially synchronous](https://groups.csail.mit.edu/tds/papers/Lynch/podc84-DLS.pdf) environment are not implemented. 
  Acceptors behave safely, but if it is necessary for them to delay receipt of messages to ensure termination, that is not implemented here.
  Such a feature may be added at a later time.
  The only proposer yet implemented uses randomized exponential backoff and keeps proposing forever.
  This is sufficient to deal with many network conditions, but will not guarantee Learner termination under all (even partially synchronous) Byzantine conditions. 
- It is not [formally verified](https://en.wikipedia.org/wiki/Formal_verification) or [model checked](https://en.wikipedia.org/wiki/Model_checking).
  Work formally verifying a version of Heterogeneous Paxos is [ongoing](https://github.com/heliaxdev/typhon/blob/master/tla/HPaxos.tla).
- The [gRPC](https://grpc.io/) interface defined is _not_ intended to be used by other, more optimized implementations. 
  Future implementations may want to include more structure or information in their messages.

## Running
### Build `het_paxos_ref`
Build with
```
cargo build
```
You can also build documentation with
```
cargo doc
```
### Create Config Files
You'll need to create some configuration files (at least 1 per Acceptor) describing the consensus setup, and the quorums for each learner.
These will also specify the public keys and addresses for each Acceptor.
You can create some config files for a simple homogeneous 4-server (all-localhost) setup using:
```
cargo run --bin generate_4_acceptor_config XXX
```
This will create 4 config files called `XXX_0_config.json`, `XXX_1_config.json`, `XXX_2_config.json`, and `XXX_3_config.json`.
(for any value of `XXX`)

Configuration file formatting is formally specified in `proto/hetpaxosrefconfig.proto`.
We use standard [JSON encoding for protocol buffers](https://developers.google.com/protocol-buffers/docs/proto3#json). 

### Launch Acceptors
Only the Acceptors run as servers. 
Everyone else connects to the Acceptors.
Therefore, we start the Acceptors first.
It does not matter what order we start them in.
Acceptors may display errors while trying to connect to Acceptors that have not started yet. 
This is ok.
When the other acceptor starts, it will establish the necessary connection.

Each acceptor needs a config file as input. 
These should differ only in the private key assigned to each Acceptor.
The file name can be specified as a command line argument:
```
cargo run --bin acceptor config.json
```

### Launch Learners
Learner processes listen to acceptors and print out statements when a Learner from the config file (just a String name) has "decided" a value. 
Learner processes are (perhaps confusingly) not actually specific to a Learner from the config file: they just print out while Learner decides whenever any Learner decides.
A Learner process needs a config file to know the quorums for each Learner, but its private key (it has to have one) doesn't actually matter.
```
cargo run --bin learner config.json
```

### Launch Proposers
You'll want to launch proposers last, so that everyone is ready when consensus actually begins. 
Proposer processes propose (the hash of) whatever value is in their config file, to all the Acceptors in their config file. 
As currently implemented, they use randomized exponential delays, and keep proposing over and over forever. 

```
cargo run --bin proposer config.json
```
Shortly after proposers launch, unless something has gone wrong, you should see Learner processes print out statements that learners have decided (hashes of) values. 
Note that, as implemented, this is not great for benchmarking, as the time it takes to establish new TLS connections interferes with the latency to establish consensus.

Currently, processes are set to print out debug information displaying all messages they send and receive. 

### Shut Down
Kill all Acceptor, Learner, and Proposer processes. 
There is no state saved outside of in-process memory.



