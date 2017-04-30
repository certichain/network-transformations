package org.protocols.paxos

import akka.actor.{ActorSystem, Props}

/**
  * @author Ilya Sergey
  */

trait PaxosFactory[T] extends PaxosVocabulary[T] {

  val AcceptorClass: Class[_]
  val LeaderClass: Class[_]
  val LearnerClass: Class[_]

  def createPaxosInstance(system: ActorSystem, numLeaders: Int,
                          numAcceptors: Int, numLearners: Int): PaxosConfiguration = {

    // Sanity checks for the configuration
    if (numLeaders <= 0) throw PaxosException(s"There should be at least one leader (currently $numLeaders)")
    if (numLearners <= 0) throw PaxosException(s"There should be at least one learner (currently $numLearners)")
    if (numAcceptors <= 0) throw PaxosException(s"Too few acceptors (currently $numAcceptors)")

    val acceptors = for (i <- 0 until numAcceptors) yield {
      system.actorOf(Props(AcceptorClass, this), name = s"Acceptor-A$i")
    }

    val leaders = for (i <- 0 until numLeaders) yield {
      system.actorOf(Props(LeaderClass, this, acceptors, i), name = s"Leader-P$i")
    }

    val learners = for (i <- 0 until numLearners) yield {
      system.actorOf(Props(LearnerClass, this, acceptors), name = s"Learner-L$i")
    }

    new PaxosConfiguration(leaders, learners, acceptors)
  }

}
