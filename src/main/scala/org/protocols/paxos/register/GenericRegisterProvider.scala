package org.protocols.paxos.register

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.protocols.paxos.PaxosException

/**
  * @author Ilya Sergey
  */
abstract class GenericRegisterProvider[T](val system: ActorSystem, val numA: Int) {

  val AcceptorClass: Class[_]
  val RegisterProxyClass: Class[_]

  // cannot be larger than the number of proposers
  private var numProposals = 1
  lazy private val acceptors = {
    // Sanity checks for the configuration
    if (numA <= 0) throw PaxosException(s"Too few acceptors (currently $numA)")
    for (i <- 0 until numA) yield {
      system.actorOf(Props(AcceptorClass, this), name = s"Acceptor-A$i")
    }
  }

  // Returns a single-served register to propose
  def getSingleServedRegister: RoundBasedRegister[T] = {
    val k = numProposals
    numProposals = numProposals + 1
    val msgQueue = new ConcurrentLinkedQueue[Any]()
    val regActor = system.actorOf(Props(RegisterProxyClass, this, msgQueue, k), name = s"RegisterMiddleman-P$k")
    new RoundBasedRegister[T](acceptors, regActor, msgQueue, k)
  }

}


