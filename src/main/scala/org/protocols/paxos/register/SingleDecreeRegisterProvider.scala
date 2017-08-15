package org.protocols.paxos.register

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.protocols.paxos.PaxosException

/**
  * @author Ilya Sergey
  */
class SingleDecreeRegisterProvider[T](val system: ActorSystem, val numA: Int) {

  /**
    * A simple actor wrapper for an acceptor only forwards the messages to the Acceptor STS and returns the results
    */
  class SingleDecreeRegisterAcceptor extends Actor {
    // An actual acceptor
    private val myAcceptorMachine = new AcceptorForRegister(self)

    override def receive: Receive = {
      // Just send all the messages by their destinations
      case m if myAcceptorMachine.step.isDefinedAt(m) =>
        val msg = myAcceptorMachine.step(m)
        msg.dest ! msg
    }
  }

  /**
    * A simple delegate for the register : only forwards the messages
    */
  class SingleDecreeRegisterProxy(acceptors: Seq[ActorRef], msgQueue: ConcurrentLinkedQueue[Any], k: Int)
      extends Actor {

    def receive: Receive = {
      case m =>
        m match {
          case msg: RegisterMessage =>
            // Redirect the message
            if (msg.dest == self) {
              msgQueue.add(msg)
            } else {
              msg.dest ! msg
            }
          case _ =>
        }
    }

  }

  val AcceptorClass: Class[_] = classOf[SingleDecreeRegisterAcceptor]
  val RegisterProxyClass: Class[_] = classOf[SingleDecreeRegisterProxy]

  // cannot be larger than the number of proposers
  private var numProposals = 1
  private val acceptors = {
    // Sanity checks for the configuration
    if (numA <= 0) throw PaxosException(s"Too few acceptors (currently $numA)")
    for (i <- 0 until numA) yield {
      system.actorOf(Props(AcceptorClass, this), name = s"Acceptor-A$i")
    }
  }

  // Returns a single-served register to propose
  def getRegisterToPropose: RoundBasedRegister[T] = {
    val k = numProposals
    numProposals = numProposals + 1
    val msgQueue = new ConcurrentLinkedQueue[Any]()
    val regActor = system.actorOf(Props(RegisterProxyClass, this, acceptors, msgQueue, k), name = s"RegisterMiddleman-P$k")
    new RoundBasedRegister[T](acceptors, regActor, msgQueue, k)
  }

}


