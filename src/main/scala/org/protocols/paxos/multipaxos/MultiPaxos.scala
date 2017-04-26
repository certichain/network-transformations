package org.protocols.paxos.multipaxos

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef}
import org.protocols.paxos.combinators.SlotProtocolCombinator

/**
  * @author Ilya Sergey
  */

trait MultiPaxos[T] extends SlotProtocolCombinator[T] {

  override type MsgType = PaxosMessage

  /**
    * Wrapping specific roles into slot-based actors
    */

  // A slot-based acceptor actor (managing all slots)
  class MultiPaxosAcceptor extends SlotBasedActor { act =>
    def createNewRoleInstance(s: Slot) = new AcceptorRole(MessageWithSlot(s, _)) {
      val self: ActorRef = act.self
    }
  }

  // A slot-based proposer actor (managing all slots)
  class MultiPaxosProposer(acceptors: Seq[ActorRef], myBallot: Ballot) extends SlotBasedActor { act =>
    def createNewRoleInstance(s: Slot) = new ProposerRole(acceptors, myBallot, MessageWithSlot(s, _)) {
      val self: ActorRef = act.self
    }
  }

  // A slot-based leader actor (managing all slots)
  class MultiPaxosLearner(acceptors: Seq[ActorRef]) extends SlotBasedActor { act =>
    def createNewRoleInstance(s: Slot): PaxosRole = new LearnerRole(acceptors, MessageWithSlot(s, _)) {
      val self: ActorRef = act.self
    }
  }

}
