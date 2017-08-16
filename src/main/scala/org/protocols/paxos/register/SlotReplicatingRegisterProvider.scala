package org.protocols.paxos.register

import akka.actor.ActorSystem

/**
  * @author Ilya Sergey
  */

class SlotReplicatingRegisterProvider [T](override val system: ActorSystem, override val numA: Int)
    extends GenericRegisterProvider[T](system, numA) {

  // Instantiate the middleware
  val AcceptorClass: Class[_] = ???
  val RegisterProxyClass: Class[_] = ???
}
