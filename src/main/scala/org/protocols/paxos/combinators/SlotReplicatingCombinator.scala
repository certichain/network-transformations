package org.protocols.paxos.combinators

import akka.actor.Actor
import org.protocols.paxos.PaxosRoles

/**
  * A class, implementing a generic functionality of an actor, involved into MultiPaxos under a specific role:
  * Proposer, Acceptor or Learner
  *
  * @author Ilya Sergey
  */


trait SlotReplicatingCombinator[T] extends PaxosRoles[T] {

  type Slot = Int
  type MsgType = PaxosMessage

  trait DisjointSlotActor extends Actor {

    import scala.collection.mutable.{Map => MMap}

    /**
      * A map from slots to the corresponding role protocols
      */
    private val slotMachineMap: MMap[Slot, PaxosRole] = MMap.empty

    protected def getMachineForSlot(slot: Slot): PaxosRole = {
      slotMachineMap.get(slot) match {
        case Some(role) => role
        case None =>
          val role = createNewRoleInstance(slot)
          slotMachineMap.update(slot, role)
          role
      }
    }

    def createNewRoleInstance(s: Slot): PaxosRole

    override def receive: Receive = {
      case MessageForSlot(slot, msg)
        if getMachineForSlot(slot).step.isDefinedAt(msg) =>
        // Get the appropriate role instance
        val roleInstance = getMachineForSlot(slot)
        val toSend = roleInstance.step(msg)
        toSend.foreach { case (a, m) => a ! MessageForSlot(slot, m) }
    }
  }
}

case class MessageForSlot[M](slot: Int, msg: M)

