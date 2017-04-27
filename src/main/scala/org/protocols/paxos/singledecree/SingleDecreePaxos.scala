package org.protocols.paxos.singledecree

import akka.actor.{Actor, ActorRef}
import org.protocols.paxos.PaxosRoles

/**
  * @author Ilya Sergey
  */

trait SingleDecreePaxos[T] extends PaxosRoles[T] {

  trait SimplePaxosRole extends Actor { this: PaxosRole =>
    override def receive: Receive = {
      // Just send all the messages by their destinations
      case msg if step.isDefinedAt(msg) => step(msg).foreach { case (a, m) => a ! m }
    }
  }

  /**
    * Wrapping roles into specific actors serving them
    */

  class SimplePaxosAcceptor extends AcceptorRole with SimplePaxosRole

  class SimplePaxosProposer(acceptors: Seq[ActorRef], myBallot: Ballot)
      extends ProposerRole(acceptors, myBallot) with SimplePaxosRole

  class SimplePaxosLearner(acceptors: Seq[ActorRef]) extends LearnerRole(acceptors) with SimplePaxosRole

}
