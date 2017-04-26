package org.protocols.paxos

import akka.actor.ActorSystem

/**
  * @author Ilya Sergey
  */

trait PaxosFactory[T] extends PaxosVocabulary[T] {

  def createPaxosInstance(system: ActorSystem, numProposers: Int,
                          numAcceptors: Int, numLearners: Int): PaxosConfiguration

}
