package org.protocols.paxos.singledecree

import akka.actor.{Actor, ActorRef}
import org.protocols.paxos.PaxosRoles

/**
  * @author Ilya Sergey
  */

trait SingleDecreePaxos[T] extends PaxosRoles[T] {

  private def id[A](t: A) = t

  trait SimplePaxosRole extends Actor { this : PaxosRole =>
    override def receive: Receive = { case msg => this.receiveHandler(msg) }
  }

  /**
    * Wrapping roles into specific actors serving them
    */

  class SimplePaxosAcceptor extends AcceptorRole(id) with SimplePaxosRole

  class SimplePaxosProposer(acceptors: Seq[ActorRef], myBallot: Ballot)
      extends ProposerRole(acceptors, myBallot, id) with SimplePaxosRole

  class SimplePaxosLearner(acceptors: Seq[ActorRef]) extends LearnerRole(acceptors, id) with SimplePaxosRole

}
