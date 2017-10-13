# A Library of Protocol Combinators

Experiments in implementing reusable consensus protocols with Scala and Akka.

This development is a proof-of-concept prototype for the work _Paxos
Consensus, Deconstructed and Abstracted_, by Alvaro Garcia-Perez,
Alexey Gotsman, Yuri Meshman1, and Ilya Sergey, currently in submission.

## How to build

Requirements: **sbt** (version >0.13.0), **JDK 1.8**. To compile and test, execute:

```
sbt test 
```

This will compile all the implementations and run the multi-threaded
tests for implemented consensus protocols.

## Family of Paxos Consensus implementations

Generic definition of Paxos roles and the corresponding implementations. 
  
* Single Degree Paxos, built on top of the Roles
* "_Fully disjoint_" Slot-replicated "MultiPaxos", using the same roles;
* "_Bunching_" MultiPaxos (this is the real MultiPaxos);
* StoppablePaxos on top of MultiPaxos (not discussed in the paper);

## Correspondence between Code and Paper

### Generic round-based register implementation

The generic register-based machinery is implemented by the classes in
the source files under `./src/main/scala/org/protocols/register`. The
following components are the essntial ones:

* `RegisterMessage.scala` - register-relevant PAxos messages, as
  described in Section 3 of the accompanying paper;

* `RoundBasedRegister.scala` - an implementation of the Acceptor and
  Proposer implementing the register-based interface;

* `RoundRegisterProvider.scala` - a generic implementation of a
  register (consensus) provider which can be extended for specific
  network semantics from Section 5. The method
  `getSingleServedRegister` is used to obtain the register, which can
  be used via its `read`, `write` and `propose` methods.

### Network semantics and register providers

Various semantics are implemented in the folders `register/singledecree` and
`register/multipaxos`:

* `SingleDecreeRegisterProvider.scala` - implementation of a
  single-decree register-based Paxos via a simple network semantics
  from Section 5.1.

* `CartesianRegisterProvider.scala` - implementation of a
  slot-replicating network layer (Section 5.3).

* `WideningSlotRegisterProvider.scala` - an optimised widening network
  layer (Section 5.5).

* `BunchingRegisterProvider.scala` - a bunching network semantics
  (Section 5.6).

### Testing different network semantics

The test suite for vairious versions of Multi-Paxos is implemented in
`protocol-combinators/src/test/scala/org/protocols/register/multipaxos/GenericRegisterMultiPaxosTests.scala`. Other
files in the same folder instantiate it with different register
providers.

Execute `sbt test` to run the test suite.
