package org.protocols.paxos.multipaxos

import akka.actor.{Actor, ActorRef}
import org.protocols.paxos.combinators.SlotProtocolCombinator

/**
  * @author Ilya Sergey
  */

trait MultiPaxos[T] extends SlotProtocolCombinator[T] {

  override type MsgType = PaxosMessage

  /**
    * Make a slot-based acceptor actor (managing all slots)
    */
  class MultiPaxosAcceptor extends SlotBasedActor {
    def createNewRoleInstance(s: Slot) = new AcceptorRole(self, MessageWithSlot(s, _))
  }

  /**
    * Make a slot-based proposer actor (managing all slots)
    */
  class MultiPaxosProposer(acceptors: Seq[ActorRef], myBallot: Ballot) extends SlotBasedActor {
    def createNewRoleInstance(s: Slot) = new ProposerRole(acceptors, myBallot, self, MessageWithSlot(s, _))
  }

  /**
    * Make a slot-based proposer actor (managing all slots)
    */
  class MultiPaxosLearner(acceptors: Seq[ActorRef], myBallot: Ballot) extends SlotBasedActor {
    def createNewRoleInstance(s: Slot): PaxosRole = new LearnerRole(acceptors, self, MessageWithSlot(s, _))
  }

}
