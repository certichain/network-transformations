package org.protocols.register.multipaxos

import akka.actor.{Actor, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap}

/**
  * @author Ilya Sergey
  */

class BunchingRegisterProvider [T](override val system: ActorSystem, override val numA: Int)
    extends WideningSlotRegisterProvider[T](system, numA) {

  class BunchingAcceptor extends WideningSlotReplicatingAcceptor {

    override def receive: Receive = {

      // TODO: Change so it would bunch
      case RegisterMessageForSlot(slot, incoming@READ(cid, j, b)) =>

        // Update the ballot and execute the result for all acceptors
        myHighestSeenBallot = Math.max(myHighestSeenBallot, b)

        // Execute phase one for *all* of the acceptors for all slots
        val actualSlots = slotAcceptorMap.keySet + slot
        for (s <- actualSlots) {
          val accInstance = getMachineForSlot(s)
          // Send back the results for all for which the result has been obtained,
          // thus short-circuiting the internal logic
          val toSend = getMachineForSlot(slot).step(incoming)
          val dst = toSend.dest
          val rms = RegisterMessageForSlot(slot, toSend)
          dst ! rms
        }

      case m => super.receive(m)

    }
  }

  class BunchingRegisterProxy(registerMap: MMap[Any, RoundBasedRegister[Any]])
      extends WideningSlotReplicatingRegisterProxy(registerMap) {

    override def receive: Receive = {
      // TODO: Add impedance matcher for bunching

      // Incoming message to a register that might or might not exist
      case rms@RegisterMessageForSlot(slot, msg: RegisterMessage) if msg.dest == self =>
        if (registerMap.isDefinedAt(slot)) {
          registerMap(slot).deliver(msg)
        } else {
          // Make a new register as by demand of this message, which came ahead of time,
          // and deliver its message
          val reg = new RoundBasedRegister[Any](acceptors, self, msg.k, slot)
          registerMap.put(slot, reg)
          reg.deliver(msg)
        }

      case m => super.receive(m)
    }

  }

  // Instantiate the middleware
  override val AcceptorClass: Class[_] = classOf[BunchingAcceptor]
  override val RegisterProxyClass: Class[_] = classOf[BunchingRegisterProxy]


}
