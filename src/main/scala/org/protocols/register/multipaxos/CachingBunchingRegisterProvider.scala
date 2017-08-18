package org.protocols.register.multipaxos

import akka.actor.{ActorRef, ActorSystem}
import org.protocols.register.{MessageToProxy, READ, RoundBasedRegister, ackREAD}

import scala.collection.concurrent.{Map => MMap, TrieMap => TMap}

/**
  * @author Ilya Sergey
  */

class CachingBunchingRegisterProvider[T](override val system: ActorSystem, override val numA: Int)
    extends BunchingRegisterProvider[T](system, numA) {


  class CachingBunchingRegisterProxy(registerMap: MMap[Any, RoundBasedRegister[Any]])
      extends BunchingRegisterProxy(registerMap) {

    // [!!!] Maintain convinced actors for the future
    private val myAcceptedValues: MMap[ActorRef, Map[Slot, Option[(Int, Any)]]] = TMap.empty

    override def receive: Receive = {
      case BunchedAcceptedValues(_, k, slotMsgs) =>
        for ((s, msg) <- slotMsgs) {
          // [!!!] Record accepted values for specific acceptors/slots to use them for short-cutting below
          msg match {
            case ackREAD(src, _, `k`, kWv) =>
              val amap = myAcceptedValues.getOrElse(src, Map())
              myAcceptedValues.put(src, amap + (s -> kWv))
            case _ =>
          }
          val reg = getRegisterForSlot(s, k)
          reg.deliver(msg)
        }

      // [!!!] This is an optional short-circuit, which avoids sending messages for new slots
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



  override val RegisterProxyClass: Class[_] = classOf[CachingBunchingRegisterProxy]


}
