package org.protocols.paxos

import akka.actor.{ActorRef, PoisonPill}

/**
  * Represent a simple paxos configuration, given to the final clients.
  *
  * Intentionally, acceptors are not exposed
  *
  * @author Ilya Sergey
  *
  */
class PaxosConfiguration(val proposers: Seq[ActorRef], val learners: Seq[ActorRef],
                         private val acceptors: Seq[ActorRef]) {

  def killAll(): Unit =
    for (a <- acceptors ++ proposers ++ learners) {
      a ! PoisonPill
    }
}

case class PaxosException(msg: String) extends Exception {
  override def getMessage: String = msg
}
