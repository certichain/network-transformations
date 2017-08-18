package org.protocols.register.multipaxos

import akka.actor.{Actor, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap}
import scala.collection.mutable.{Set => MSet}

/**
  * @author Ilya Sergey
  */

class WideningSlotRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends CartesianRegisterProvider[T](system, numA) {

  class WideningSlotReplicatingAcceptor extends CartesianAcceptor {

    protected var myHighestSeenBallot: Int = -1

    protected override def getMachineForSlot(slot: Slot): AcceptorForRegister = {
      val acc = slotAcceptorMap.get(slot) match {
        case Some(role) => role
        case None =>
          val role = new AcceptorForRegister(self)
          slotAcceptorMap.update(slot, role)
          role
      }
      acc.bumpUpBallot(myHighestSeenBallot)
      slotAcceptorMap.update(slot, acc)
      acc
    }


    override def receive: Receive = {
      case RegisterMessageForSlot(slot, incoming@READ(cid, j, b)) =>

        // Update the ballot and execute the result for all acceptors
        myHighestSeenBallot = Math.max(myHighestSeenBallot, b)

        // Execute phase one for *all* of the acceptors for all slots
        val actualSlots = slotAcceptorMap.keySet + slot
        for (s <- actualSlots) {
          val toSend = getMachineForSlot(s).step(incoming)
          val dst = toSend.dest
          val rms = RegisterMessageForSlot(s, toSend)
          dst ! rms
        }

      case m => super.receive(m)

    }

  }

  /**
    * A Proxy that accepts slot-marked messages
    */
  class WideningSlotReplicatingRegisterProxy(registerMap: MMap[Any, RoundBasedRegister[Any]])
      extends CartesianRegisterProxy(registerMap) {

    override def receive: Receive = {
      // Incoming message to a register that might or might not exist
      case rms@RegisterMessageForSlot(slot, msg: RegisterMessage) if msg.dest == self =>
        val reg = getRegisterForSlot(slot, msg.k)

        reg.deliver(msg)

      case m => super.receive(m)
    }

    protected def getRegisterForSlot(slot: Slot, k: Int) = {
      if (registerMap.isDefinedAt(slot)) {
        registerMap(slot)
      } else {
        // Make a new register as by demand of this message, which came ahead of time,
        // and deliver its Phase-1 message
        val reg = new RoundBasedRegister[Any](acceptors, self, k, slot)
        registerMap.put(slot, reg)
        reg
      }
    }
  }

  // Instantiate the middleware
  override val AcceptorClass: Class[_] = classOf[WideningSlotReplicatingAcceptor]
  override val RegisterProxyClass: Class[_] = classOf[WideningSlotReplicatingRegisterProxy]

}
