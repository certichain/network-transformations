package org.protocols.register.multipaxos

import akka.actor.{Actor, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap}
import scala.collection.mutable.{Set => MSet}

/**
  * @author Ilya Sergey
  */

class WideningSlotRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends SlotReplicatingRegisterProvider[T](system, numA) {

  class WideningSlotReplicatingAcceptor extends SlotReplicatingAcceptor {

    private var myHighestSeenBallot: Int = -1
    private val sentMessages: MSet[RegisterMessageForSlot[Any]] = MSet.empty

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

  /**
    * A Proxy that accepts slot-marked messages
    */
  class WideningSlotReplicatingRegisterProxy(registerMap: MMap[Any, RoundBasedRegister[Any]]) extends Actor {
    def receive: Receive = {
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

      // Outgoing message
      case MessageToProxy(msg: RegisterMessage, contextParam: Any) =>
        assert(contextParam.isInstanceOf[Int])
        val slot = contextParam.asInstanceOf[Int]
        msg.dest ! RegisterMessageForSlot(slot, msg)
      case _ =>
    }

  }

  // Instantiate the middleware
  override val AcceptorClass: Class[_] = classOf[WideningSlotReplicatingAcceptor]
  override val RegisterProxyClass: Class[_] = classOf[WideningSlotReplicatingRegisterProxy]

}
