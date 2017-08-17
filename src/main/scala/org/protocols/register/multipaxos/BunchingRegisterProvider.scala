package org.protocols.register.multipaxos

import akka.actor.{ActorRef, ActorSystem}
import org.protocols.register._

import scala.collection.concurrent.{Map => MMap, TrieMap => TMap}

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

    // Maintain convinced actors for the future
    protected val myAcceptedValues: MMap[ActorRef, Map[Slot, Option[(Int, Any)]]] = TMap.empty

    override def receive: Receive = {
      case BunchedAcceptedValues(_, k, slotMsgs) =>
        for ((s, msg) <- slotMsgs) {
          // Record accepted values for specific acceptors/slots to use them for short-cutting below
          msg match {
            case ackREAD(src, _, `k`, kWv) =>
              val amap = myAcceptedValues.getOrElse(src, Map())
              myAcceptedValues.put(src, amap + (s -> kWv))
            case _ =>
          }
          val reg = getRegisterForSlot(s, k)
          reg.deliver(msg)
        }

      // This is an optional short-circuit, which avoids sending messages for new slots
      case MessageToProxy(READ(_, j, k), slot: Int)
        if myAcceptedValues.isDefinedAt(j) &&
            myAcceptedValues(j).isDefinedAt(slot) &&
            myAcceptedValues(j)(slot).isDefined =>
        val reg = getRegisterForSlot(slot, k)
        val vW = myAcceptedValues(j)(slot)
        reg.deliver(ackREAD(j, self, k, vW))

      case m => super.receive(m)
    }

  }

  // Instantiate the middleware
  override val AcceptorClass: Class[_] = classOf[BunchingAcceptor]
  override val RegisterProxyClass: Class[_] = classOf[BunchingRegisterProxy]

}

// A message type for a bunched acceptor response
case class BunchedAcceptedValues(a: ActorRef, k: Int, slotVals: Seq[(Int, RegisterMessage)])

