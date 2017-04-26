package org.protocols.paxos.singledecree

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}

/**
  * @author Ilya Sergey
  */

class SingleDecreePaxosFactory[T] extends SingleDecreePaxos[T] {

  /**
    * Represent a simple paxos configuration, given to the final clients.
    *
    * Intentionally, acceptors are not exposed
    *
    */
  class SimplePaxosConfiguration(val proposers: Seq[ActorRef], val learners: Seq[ActorRef],
                                 private val acceptors: Seq[ActorRef]) {

    def killAll(): Unit =
      for (a <- acceptors ++ proposers ++ learners) {
        a ! PoisonPill
      }
  }

  def createPaxosSimplePaxosInstance(system: ActorSystem, numProposers: Int,
                                     numAcceptors: Int, numLearners: Int): SimplePaxosConfiguration = {

    // Sanity checks for the configuration
    if (numProposers <= 0) throw SimpleDecreePaxosException(s"There should be at least one proposer (currently $numProposers)")
    if (numLearners <= 0) throw SimpleDecreePaxosException(s"There should be at least one learner (currently $numLearners)")
    if (numAcceptors <= 0) throw SimpleDecreePaxosException(s"Too few acceptors (currently $numAcceptors)")

    val acceptors = for (i <- 0 until numAcceptors) yield {
      system.actorOf(Props(classOf[Acceptor], this), name = s"Acceptor-A$i")
    }

    val proposers = for (i <- 0 until numProposers) yield {
      system.actorOf(Props(classOf[Proposer], this, acceptors, i), name = s"Proposer-P$i")
    }

    val learners = for (i <- 0 until numLearners) yield {
      system.actorOf(Props(classOf[Learner], this, acceptors), name = s"Learner-L$i")
    }

    new SimplePaxosConfiguration(proposers, learners, acceptors)
  }

  // TODO 3: factor out the phases, so we could combine the messages

  // TODO 4: implement the "blend "combinator


}

case class SimpleDecreePaxosException(msg: String) extends Exception {
  override def getMessage: String = msg
}
