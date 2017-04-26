package org.protocols.paxos.multipaxos

import org.protocols.paxos.PaxosFactory

/**
  * @author Ilya Sergey
  */

class MultiPaxosFactory[T] extends PaxosFactory[T] with MultiPaxos[T] {
  val AcceptorClass: Class[_] = classOf[MultiPaxosAcceptor]
  val ProposerClass: Class[_] = classOf[MultiPaxosProposer]
  val LearnerClass: Class[_] = classOf[MultiPaxosLearner]
}

