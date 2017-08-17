package org.protocols.register.multipaxos

import akka.actor.{Actor, ActorRef, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap}

/**
  * @author Ilya Sergey
  */

class BunchingRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends WideningSlotRegisterProvider[T](system, numA) {

  class BunchingAcceptor extends WideningSlotReplicatingAcceptor {

    override def receive: Receive = {

      case RegisterMessageForSlot(slot, incoming@READ(cid, j, k)) =>

        myHighestSeenBallot = Math.max(myHighestSeenBallot, k)
        val actualSlots = slotAcceptorMap.keySet + slot

        val slotResults: Seq[(Slot, RegisterMessage)] =
          (for (s <- actualSlots) yield {
            val toSend = getMachineForSlot(s).step(incoming)
            (s, toSend)
          }).toSeq

        // All have the same suggested k
        assert(slotResults.forall(sr => sr._2.k == k))
        // All have the same destination
        assert(slotResults.forall(sr => sr._2.dest == cid))
        // All are Phase 1-responses
        assert(slotResults.forall(sr => sr._2.isInstanceOf[ackREAD] || sr._2.isInstanceOf[nackREAD]))

        cid ! BunchedAcceptedValues(self, k, slotResults)

      case m => super.receive(m)

    }
  }

  class BunchingRegisterProxy(registerMap: MMap[Any, RoundBasedRegister[Any]])
      extends WideningSlotReplicatingRegisterProxy(registerMap) {

    override def receive: Receive = {
      case BunchedAcceptedValues(_, k, slotMsgs) =>
        for ((s, msg) <- slotMsgs) {
          val reg = getRegisterForSlot(s, k)
          reg.deliver(msg)
        }

      // TODO: Short-circuit needless read-requests

      case m => super.receive(m)
    }

  }

  // Instantiate the middleware
  override val AcceptorClass: Class[_] = classOf[BunchingAcceptor]
  override val RegisterProxyClass: Class[_] = classOf[BunchingRegisterProxy]

}

// A message type for a bunched acceptor response
case class BunchedAcceptedValues(a: ActorRef, k: Int, slotVals: Seq[(Int, RegisterMessage)])

