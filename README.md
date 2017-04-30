# A Library of Protocol Combinators

Experiments in implementing reusable distributed protocols with Scala and Akka.

## Family of Paxos Consensus implementations

Generic definition of Paxos roles and the corresponding implementations.  
  
* Single Degree Paxos, built on top of the Roles
* "_Fully disjoint_" MultiPaxos, using the same roles
* "_Bunching_" MultiPaxos (this is the real MultiPaxos)
* StoppablePaxos on top of MultiPaxos

### Other case studies for combining messages

* Mencius

## Additional protocols

Contains simplistic implementation of the following protocols and tests for them:

* 2/3 decentralized consensus
* Two-Phase Commit (with a centralized coordinator)

You will need the SBT tool. To build and test, run `sbt test`.
