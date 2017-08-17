package org.protocols.register.multipaxos

import akka.actor.ActorSystem
import org.protocols.register.{AcceptorForRegister, READ}

/**
  * @author Ilya Sergey
  */

class WideningSlotRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends SlotReplicatingRegisterProvider[T](system, numA) {

  class WideningSlotReplicatingAcceptor extends SlotReplicatingAcceptor {

    private var myHighestSeenBallot: Int = -1

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

        // Execute phase one for all of the acceptors
        for (s <- slotAcceptorMap.keySet + slot) {
          val accInstance = getMachineForSlot(s)
          // Send back the results for all for which the result has been obtained,
          // thus short-circuiting the internal logic
          val toSend = getMachineForSlot(slot).step(incoming)
          val dst = toSend.dest
          dst ! RegisterMessageForSlot(slot, toSend)
        }

      case m => super.receive(m)

    }

  }


}
