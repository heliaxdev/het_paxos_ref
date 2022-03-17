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

## Code Layout
The basic consensus design is intended to mirror [the Technical Report](https://arxiv.org/abs/2011.08253).
The `atomic` block from `acceptor_on_receipt` in Figure 3 of [the Technical Report](https://arxiv.org/abs/2011.08253) is implemented fairly directly as `AcceptorState::deliver_message`.
However, several of the properties of consensus messages (e.g. which Acceptors are caught) are pre-computed in `src/parsed_message.rs`, as re-computing them every time can create exponential runtimes. 

In order to ensure Acceptors and Learners process messages atomically, each has a single [mutex](https://en.wikipedia.org/wiki/Mutual_exclusion#Types_of_mutual_exclusion_devices) which must be locked while processing a message. 
This ensures messages are processed serially.
It is not, however, very efficient, or strictly necessary. 

### [Tonic](https://docs.rs/tonic/latest/tonic/) [gRPC](https://grpc.io/) Stuff
Acceptors implement servers specified in `proto/hetpaxosref.proto`.
Specifically, they accept [gRPC](https://grpc.io/) connections allowing infinite streams of `ConsensusMessage`s in both directions. 
Technically, the behaviour for what they do when initiating such a [gRPC](https://grpc.io/) call is specified under `impl acceptor_server::Acceptor for Acceptor` in `src/acceptor.rs`. 
In practice, we process each incoming message (both from an incoming  [gRPC](https://grpc.io/) call and as a response to an outgoing [gRPC](https://grpc.io/) call) separately and concurrently, using `AcceptorMutex::receive_message` in `src/acceptor.rs`.
Learners are similarly designed to process each incoming message separately and concurrently, using `LearnerMutex::receive_message` in `src/learner.rs`.
There is non-trivial code re-use between Learners and Acceptors, but leaving them separate makes the implementation of each easier to understand in isolation.

At startup, Acceptors, Learners, and Proposers all try to establish ongoing [gRPC](https://grpc.io/) calls with all Acceptors.
This ensures that, regardless of startup order, all pairs of Acceptors should be able to establish a successful [gRPC](https://grpc.io/) call in at least one direction. 

## Messages
We try to pre-compute many of the functions from [the Technical Report](https://arxiv.org/abs/2011.08253) when we receive a message (re-computing them every time we need them can cause exponential runtimes). 
We do this by constructing a `ParsedMessage` from a `ConsensusMessage` using `ParsedMessage::new` (which returns `None` if the `ConsensusMessage` is not _well-formed_).
For this reason, `ParsedMessage::new` contains much of the "meat" of the implementation.

Unlike [the Technical Report](https://arxiv.org/abs/2011.08253), our _2A_ messages do not specify a Learner. 
Instead, we encode 2A and 1B messages simply as a signed set of references (hashes), and determine at parsing time if they form a valid 1B or 2A. 
In this way, a single `ConsensusMessage` can represent multiple _2A_ messages, for different Learners. 

## Ballot Numbers
As per [the Technical Report](https://arxiv.org/abs/2011.08253), Ballot numbers must be unique to the value proposed.
Since termination can require a higher ballot number than previously used, it helps if they increase with time. 
Our Ballot numbers are therefore pairs: a [Timestamp](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp), and a hash of the proposed value.
Acceptors delay delivery of any _1A_ until their local clock has passed the _1A_'s Timestamp (this is implemented in `AcceptorMutex::receive_message`).
If all clocks are synchronized, then in some sense, the "best" a Proposer can do is to use the current time in its Ballots.
Even when all clocks are not synchronized, all _1A_s sent will _eventually_ be received. 

