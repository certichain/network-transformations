package org.protocols.paxos.multipaxos.disjoint

import akka.actor.ActorRef
import org.protocols.paxos.PaxosFactory
import org.protocols.paxos.combinators.SlotReplicatingCombinator

/**
  * @author Ilya Sergey
  */

trait DisjointMultiPaxos[T] extends SlotReplicatingCombinator[T] {

  override type MsgType = PaxosMessage

  /**
    * Wrapping specific roles into slot-based actors
    */

  // A slot-based acceptor actor (managing all slots)
  class MultiPaxosAcceptor extends DisjointSlotActor { act =>
    override type Role = AcceptorRole
    def createNewRoleInstance(s: Slot) = createAcceptor(act.self)
  }

  // A slot-based proposer actor (managing all slots)
  class MultiPaxosProposer(acceptors: Seq[ActorRef], myBallot: Ballot) extends DisjointSlotActor { act =>
    override type Role = ProposerRole
    def createNewRoleInstance(s: Slot) = createProposer(acceptors, myBallot, act.self)
  }

  // A slot-based leader actor (managing all slots)
  class MultiPaxosLearner(acceptors: Seq[ActorRef]) extends DisjointSlotActor { act =>
    override type Role = LearnerRole
    def createNewRoleInstance(s: Slot) = createLearner(acceptors, self)
  }

}

class DisjointMultiPaxosFactory[T] extends PaxosFactory[T] with DisjointMultiPaxos[T] {
  val AcceptorClass: Class[_] = classOf[MultiPaxosAcceptor]
  val ProposerClass: Class[_] = classOf[MultiPaxosProposer]
  val LearnerClass: Class[_] = classOf[MultiPaxosLearner]
}

