package org.protocols.register.multipaxos

import akka.actor.{Actor, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap, TrieMap => TMap}

/**
  * @author Ilya Sergey
  */

class SlotReplicatingRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends RoundRegisterProvider[T](system, numA) {

  type Slot = Int

  // Replicating acceptor (one acceptor maintains multiple slots)
  class SlotReplicatingAcceptor extends Actor {
    protected val slotAcceptorMap: MMap[Slot, AcceptorForRegister] = TMap.empty

    protected def getMachineForSlot(slot: Slot): AcceptorForRegister = {
      slotAcceptorMap.get(slot) match {
        case Some(role) => role
        case None =>
          val role = new AcceptorForRegister(self)
          slotAcceptorMap.update(slot, role)
          role
      }
    }

    protected def getAllMachines: Map[Slot, AcceptorForRegister] =
      (for (s <- slotAcceptorMap.keys) yield (s, getMachineForSlot(s))).toMap

    override def receive: Receive = {
      case RegisterMessageForSlot(slot, incoming)
        if getMachineForSlot(slot).step.isDefinedAt(incoming) =>
        val toSend = getMachineForSlot(slot).step(incoming)
        val dst = toSend.dest
        dst ! RegisterMessageForSlot(slot, toSend)
    }
  }


  class SlotReplicatingRegisterProxy(registerMap: MMap[Any, RoundBasedRegister[Any]]) extends Actor {
    def receive: Receive = {
      // Incoming message
      case rms@RegisterMessageForSlot(slot, msg: RegisterMessage)
        // Do not react to the slots that haven't been requested yet
        if msg.dest == self && registerMap.isDefinedAt(slot) =>
        // TODO: in the future we can also create our own registers right here
        registerMap(slot).putMsg(msg)

      // Outgoing message
      case MessageToProxy(msg: RegisterMessage, contextParam: Any) =>
        assert(contextParam.isInstanceOf[Int])
        val slot = contextParam.asInstanceOf[Int]
        msg.dest ! RegisterMessageForSlot(slot, msg)
      case _ =>
    }

  }

  // Instantiate the middleware
  val AcceptorClass: Class[_] = classOf[SlotReplicatingAcceptor]
  val RegisterProxyClass: Class[_] = classOf[SlotReplicatingRegisterProxy]
}

case class RegisterMessageForSlot[+M](slot: Int, msg: M)
