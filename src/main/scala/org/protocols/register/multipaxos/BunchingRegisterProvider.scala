package org.protocols.register.multipaxos

import akka.actor.{ActorRef, ActorSystem}
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

    // Maintain convinced actors for the future
    protected var myConvincedWithNoValue: Map[Int, ActorRef] = Map.empty
    protected var servedSlots: Set[Int] = Set.empty

    override def receive: Receive = {
      case BunchedAcceptedValues(_, k, slotMsgs) =>
        for ((s, msg) <- slotMsgs) {
          msg match {
            case ackREAD(src, _, `k`, kWv) => myConvincedWithNoValue = myConvincedWithNoValue + (k -> src)
            case _ =>
          }
          servedSlots = servedSlots + s
          val reg = getRegisterForSlot(s, k)
          reg.deliver(msg)
        }

      // This is an optional short-circuit, which avoids sending messages for new slots
      case MessageToProxy(READ(_, j, k), slot: Int)
        if myConvincedWithNoValue.isDefinedAt(k) &&
            !servedSlots.contains(slot) =>
        val reg = getRegisterForSlot(slot, k)
        reg.deliver(ackREAD(j, self, k, None))
        servedSlots = servedSlots + slot

      case m => super.receive(m)
    }

  }

  // Instantiate the middleware
  override val AcceptorClass: Class[_] = classOf[BunchingAcceptor]
  override val RegisterProxyClass: Class[_] = classOf[BunchingRegisterProxy]

}

// A message type for a bunched acceptor response
case class BunchedAcceptedValues(a: ActorRef, k: Int, slotVals: Seq[(Int, RegisterMessage)])

