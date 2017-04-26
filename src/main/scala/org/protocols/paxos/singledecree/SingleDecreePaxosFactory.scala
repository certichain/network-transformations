package org.protocols.paxos.singledecree

import akka.actor.{ActorSystem, Props}
import org.protocols.paxos.{PaxosConfiguration, PaxosException, PaxosFactory}

/**
  * @author Ilya Sergey
  */

class SingleDecreePaxosFactory[T] extends SingleDecreePaxos[T] with PaxosFactory[T] {

  override def createPaxosInstance(system: ActorSystem, numProposers: Int,
                                   numAcceptors: Int, numLearners: Int): PaxosConfiguration = {

    // Sanity checks for the configuration
    if (numProposers <= 0) throw PaxosException(s"There should be at least one proposer (currently $numProposers)")
    if (numLearners <= 0) throw PaxosException(s"There should be at least one learner (currently $numLearners)")
    if (numAcceptors <= 0) throw PaxosException(s"Too few acceptors (currently $numAcceptors)")

    val acceptors = for (i <- 0 until numAcceptors) yield {
      system.actorOf(Props(classOf[Acceptor], this), name = s"Acceptor-A$i")
    }

    val proposers = for (i <- 0 until numProposers) yield {
      system.actorOf(Props(classOf[Proposer], this, acceptors, i), name = s"Proposer-P$i")
    }

    val learners = for (i <- 0 until numLearners) yield {
      system.actorOf(Props(classOf[Learner], this, acceptors), name = s"Learner-L$i")
    }

    new PaxosConfiguration(proposers, learners, acceptors)
  }

}

