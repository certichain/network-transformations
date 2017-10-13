# Combinators for Distributed Consensus

This project provides a library of reusable components to build
distributed consensus protocols of the Paxos family, implemented in
Scala and Akka.

The development in this repository is a proof-of-concept prototype for
the work _Paxos Consensus, Deconstructed and Abstracted_, by Alvaro
Garcia-Perez, Alexey Gotsman, Yuri Meshman, and Ilya Sergey, which is
currently in submission.

## How to build

Requirements: **sbt** (version >0.13.0), **JDK 1.8**. To compile and test, execute:

```
sbt test 
```

This will compile all the implementations and run the multi-threaded
tests for implemented consensus protocols.

## Family of Paxos Consensus Implementations

Generic definition of Paxos roles and the corresponding implementations. 
  
* Single Degree Paxos, built on top of a single-decree round-based register
* "_Fully disjoint_" Slot-replicated Paxos, using the SD RB Register;
* "_Widened_" Slot-replicated Paxos with "spurious" Phase1a messages;
* "_Bunching_" MultiPaxos with "bunched" speculative Phase1a messages
  (this is the real MultiPaxos);
* StoppablePaxos on top of MultiPaxos (not discussed in the paper);

## Correspondence between Code and Paper

### Generic round-based register implementation

The generic register-based machinery is implemented by the classes in
the source files under [`./src/main/scala/org/protocols/register`](src/main/scala/org/protocols/register). The
following components are the essential ones:

* [`RegisterMessage.scala`](src/main/scala/org/protocols/register/RegisterMessage.scala) - register-relevant Paxos messages, as
  described in Section 3 of the accompanying paper;

* [`RoundBasedRegister.scala`](src/main/scala/org/protocols/register/RoundBasedRegister.scala) - an implementation of the Acceptor and
  Proposer corresponding the register-based interface of a
  Single-Decree Paxos (Section 3);

* [`RoundRegisterProvider.scala`](src/main/scala/org/protocols/register/RoundRegisterProvider.scala) - a generic implementation of a
  register (consensus) provider which can be extended for specific
  network semantics from Section 5. The method
  `getSingleServedRegister` is used to obtain the register, which can
  be used via its `read`, `write` and `propose` methods (Section 6).

### Network semantics and register providers

Various semantics are implemented in the folders `register/singledecree` and
`register/multipaxos`:

* [`SingleDecreeRegisterProvider.scala`](src/main/scala/org/protocols/register/singledecree/SingleDecreeRegisterProvider.scala) - implementation of a
  single-decree register-based Paxos via a simple network semantics
  from Section 5.1.

* [`CartesianRegisterProvider.scala`](src/main/scala/org/protocols/register/multipaxos/CartesianRegisterProvider.scala) - implementation of a slot-replicating network layer (Section 5.3).

* [`WideningSlotRegisterProvider.scala`](src/main/scala/org/protocols/register/multipaxos/WideningSlotRegisterProvider.scala) - an optimised widening network layer (Section 5.5).

* [`BunchingRegisterProvider.scala`](src/main/scala/org/protocols/register/multipaxos/BunchingRegisterProvider.scala) - a bunching network semantics (Section 5.6).

### Testing different network semantics

The test suite for various versions of Multi-Paxos is implemented in
[`./src/test/scala/org/protocols/register/multipaxos`](./src/test/scala/org/protocols/register/multipaxos):

* [`GenericRegisterMultiPaxosTests.scala`](src/test/scala/org/protocols/register/multipaxos/GenericRegisterMultiPaxosTests.scala) - a set of tests for all versions of MultiPaxos, parameterised by the provider;

Other files in the same folder instantiate it with different register
providers.

Execute `sbt test` to run the test suite.
