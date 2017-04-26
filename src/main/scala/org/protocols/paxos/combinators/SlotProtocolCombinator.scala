package org.protocols.paxos.combinators

import akka.actor.Actor
import org.protocols.paxos.PaxosRoles

/**
  * A class, implementing a generic functionality of an actor, involved into MultiPaxos under a specific role:
  * Proposer, Acceptor or Learner
  *
  * @author Ilya Sergey
  */


trait SlotProtocolCombinator[T] extends PaxosRoles[T] {

  type Slot = Int
  type MsgType = PaxosMessage

  case class MessageWithSlot(slot: Int, msg: MsgType)

  trait SlotBasedActor extends Actor {

    import scala.collection.mutable.{Map => MMap}

    /**
      * A map from slots to the corresponding role protocols
      */
    private val slotMachineMap: MMap[Slot, PaxosRole] = MMap.empty

    override def receive: Receive = {
      case MessageWithSlot(slot, msg) =>
        // Get the appropriate role instance
        val roleInstance = slotMachineMap.get(slot) match {
          case Some(role) => role
          case None =>
            val role = createNewRoleInstance(slot)
            slotMachineMap.update(slot, role)
            role
        }
        roleInstance.receiveHandler(msg)
    }

    def createNewRoleInstance(s: Slot): PaxosRole
  }

}
