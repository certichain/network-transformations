package org.protocols.paxos.register

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{ActorSystem, Props}
import org.protocols.paxos.PaxosException

import scala.collection.mutable.{Map => MMap}

/**
  * @author Ilya Sergey
  */
abstract class RoundRegisterProvider[T](val system: ActorSystem, val numA: Int) {

  val AcceptorClass: Class[_]
  val RegisterProxyClass: Class[_]

  val registerMap: MMap[String, RoundBasedRegister[T]] = MMap.empty

  // cannot be larger than the number of proposers
  lazy private val acceptors = {
    // Sanity checks for the configuration
    if (numA <= 0) throw PaxosException(s"Too few acceptors (currently $numA)")
    for (i <- 0 until numA) yield {
      system.actorOf(Props(AcceptorClass, this), name = s"Acceptor-A$i")
    }
  }

  /**
    * @param params Here, we're exploiting damn dynamic reflection by passing params as Seq[Any]
    *               in order to account for both single-decree and multi-decree case. Therefore, the convention is that
    *               the first argument should always be k, i.e., the supposed ballot.
    * @return single-served register to propose
    */
  def getSingleServedRegister(params: Any*): RoundBasedRegister[T] = {
    val msgQueue = new ConcurrentLinkedQueue[Any]()

    assert(params.nonEmpty, s"Parameter sequence is empty!")

    // Get the proxy for the given ID
    val proxyId = params.mkString("-")
    if (registerMap.contains(proxyId)) {
      registerMap(proxyId)
    } else {
      // the proxy doesn't care about the ballot, so we only pass the tails of params
      val act = system.actorOf(Props(RegisterProxyClass, this, msgQueue, params.tail),
        name = s"RegisterMiddleman-P$proxyId")

      val p0 = params.head
      assert(p0.isInstanceOf[Int], s"First parameter must be a ballot of type Int: $p0")
      val k = p0.asInstanceOf[Int]
      val reg = new RoundBasedRegister[T](acceptors, act, msgQueue, k)

      registerMap.put(proxyId, reg)
      reg
    }
  }
  
}

