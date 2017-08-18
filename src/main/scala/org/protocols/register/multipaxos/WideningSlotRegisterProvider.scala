package org.protocols.register.multipaxos

import akka.actor.{ActorRef, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap}

/**
  * @author Ilya Sergey
  */

class WideningSlotRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends CartesianRegisterProvider[T](system, numA) {

  class WideningSlotReplicatingAcceptor extends CartesianAcceptor {

    protected var mySupportedRequest: Option[(Int, ActorRef)] = None

    protected override def getMachineForSlot(slot: Slot): AcceptorForRegister = {
      val acc = slotAcceptorMap.get(slot) match {
        case Some(role) => role
        case None =>
          val role = new AcceptorForRegister(self)
          slotAcceptorMap.put(slot, role)
          role
      }

      // Make the acceptor agree on the message
      mySupportedRequest match {
        case Some((k, from)) => acc.step(READ(from, self, k))
        case _ =>
      }
      acc
    }

    protected def updateSupportedProposer(cid: ActorRef, b: Int) =
      mySupportedRequest match {
        case Some((k, j)) if k >= b =>
        case _ => mySupportedRequest = Some(b, cid)
      }

    override def receive: Receive = {
      case RegisterMessageForSlot(slot, incoming@READ(cid, j, b)) =>

        // Update the ballot and execute the result for all acceptors
        updateSupportedProposer(cid, b)

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
