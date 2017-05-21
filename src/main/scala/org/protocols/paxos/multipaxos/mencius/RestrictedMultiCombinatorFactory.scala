package org.protocols.paxos.multipaxos.mencius

import akka.actor.{ActorRef, ActorSystem, Props}
import org.protocols.paxos.combinators.{CommandOrNoOp, RestrictedCombinator}
import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxosFactory

import scala.collection.immutable.IndexedSeq

/**
  * @author Ilya Sergey
  */

class RestrictedMultiCombinatorFactory[T] extends DisjointMultiPaxosFactory[CommandOrNoOp[T]]
    with RestrictedCombinator[T] {

  override val ProposerClass: Class[_] = classOf[RestrictedBunchingActor]

  override protected def createProposers(system: ActorSystem, numProposers: Ballot, acceptors: IndexedSeq[ActorRef]) =
    for (i <- 0 until numProposers) yield {
      system.actorOf(Props(ProposerClass, this, acceptors, i, numProposers), name = s"Proposer-P$i")
    }

}
