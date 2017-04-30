package org.protocols.paxos.singledecree.monolithic

import org.protocols.paxos.PaxosFactory

/**
  * @author Ilya Sergey
  */

class MonolithicSDPaxosFactory[T] extends MonolithicSingleDecreePaxos[T] with PaxosFactory[T] {

  val AcceptorClass: Class[_] = classOf[Acceptor]
  val ProposerClass: Class[_] = classOf[Proposer]
  val LearnerClass: Class[_] = classOf[Learner]

}

