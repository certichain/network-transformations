package org.protocols.paxos.singledecree

import org.protocols.paxos.PaxosFactory

/**
  * @author Ilya Sergey
  */

class SingleDecreePaxosFactory[T] extends SingleDecreePaxos[T] with PaxosFactory[T] {

  val AcceptorClass: Class[_] = classOf[SimplePaxosAcceptor]
  val ProposerClass: Class[_] = classOf[SimplePaxosProposer]
  val LearnerClass: Class[_] = classOf[SimplePaxosLearner]

}

