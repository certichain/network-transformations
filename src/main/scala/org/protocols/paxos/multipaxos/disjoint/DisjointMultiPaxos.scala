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
    def createNewRoleInstance(s: Slot) = new AcceptorRole() {
      val self: ActorRef = act.self
    }
  }

  // A slot-based proposer actor (managing all slots)
  class MultiPaxosProposer(acceptors: Seq[ActorRef], myBallot: Ballot) extends DisjointSlotActor { act =>
    def createNewRoleInstance(s: Slot) = new ProposerRole(acceptors, myBallot) {
      val self: ActorRef = act.self
    }
  }

  // A slot-based leader actor (managing all slots)
  class MultiPaxosLearner(acceptors: Seq[ActorRef]) extends DisjointSlotActor { act =>
    def createNewRoleInstance(s: Slot): PaxosRole = new LearnerRole(acceptors) {
      val self: ActorRef = act.self
    }
  }

}

class DisjointMultiPaxosFactory[T] extends PaxosFactory[T] with DisjointMultiPaxos[T] {
  val AcceptorClass: Class[_] = classOf[MultiPaxosAcceptor]
  val ProposerClass: Class[_] = classOf[MultiPaxosProposer]
  val LearnerClass: Class[_] = classOf[MultiPaxosLearner]
}

