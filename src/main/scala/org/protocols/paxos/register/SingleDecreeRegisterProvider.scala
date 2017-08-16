package org.protocols.paxos.register

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorRef, ActorSystem}

/**
  * @author Ilya Sergey
  */

class SingleDecreeRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends GenericRegisterProvider[T](system, numA) {

  /**
    * A simple actor wrapper for an acceptor only forwards the messages to the Acceptor STS and returns the results
    */
  class SingleDecreeAcceptor extends Actor {
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
  class SingleDecreeRegisterProxy(msgQueue: ConcurrentLinkedQueue[Any], k: Int) extends Actor {

    def receive: Receive = {
      case m => m match {
        case msg: RegisterMessage =>
          if (msg.dest == self) {
            msgQueue.add(msg) // Incoming message
          } else {
            msg.dest ! msg // Outgoing message
          }
        case _ =>
      }
    }

  }

  val AcceptorClass: Class[_] = classOf[SingleDecreeAcceptor]
  val RegisterProxyClass: Class[_] = classOf[SingleDecreeRegisterProxy]

}



