package org.protocols.register

import akka.actor.{ActorRef, ActorSystem, Props}
import org.protocols.paxos.PaxosException

import scala.collection.concurrent.{Map => MMap, TrieMap => TMap}

/**
  * @author Ilya Sergey
  */
abstract class RoundRegisterProvider[T](val system: ActorSystem, val numA: Int) {

  val AcceptorClass: Class[_]
  val RegisterProxyClass: Class[_]

  val proxyMap: MMap[Int, ActorRef] = TMap.empty
  val globalRegisterMap: MMap[ActorRef, MMap[Any, RoundBasedRegister[T]]] = TMap.empty

  // cannot be larger than the number of proposers
  lazy protected val acceptors = {
    // Sanity checks for the configuration
    if (numA <= 0) throw PaxosException(s"Too few acceptors (currently $numA)")
    for (i <- 0 until numA) yield {
      system.actorOf(Props(AcceptorClass, this), name = s"Acceptor-A$i")
    }
  }

  /**
    * @param contextParam Here, we're exploiting damn dynamic reflection by passing params Any,
    *                     so it might be both Unit (for SD Paxos) and Slot (for Multi-Paxos).
    *                     This way we can account for both single-decree and multi-decree case.

    * @return single-served round-based register to propose through
    */
  def getSingleServedRegister(k: Int, contextParam: Any = ()): RoundBasedRegister[T] = {
    // Try to retrieve the register
    val registerCode = contextParam

    // Case 1: Register (and its proxy) already exist
    if (proxyMap.isDefinedAt(k) &&
        globalRegisterMap(proxyMap(k)).isDefinedAt(registerCode)) {
      return globalRegisterMap(proxyMap(k))(registerCode)
    }

    // Case 2: Proxy actor exists, but not the register
    if (proxyMap.isDefinedAt(k) &&
        !globalRegisterMap(proxyMap(k)).isDefinedAt(registerCode)) {
      val aref = proxyMap(k)
      val register = new RoundBasedRegister[T](acceptors, aref, k, contextParam)
      globalRegisterMap(proxyMap(k)).put(registerCode, register)
      return register
    }

    // Case 3: Proxy actor doesn't exist (and hence neither does the register)
    val mmap: MMap[Any, RoundBasedRegister[T]] = TMap.empty
    val aref = system.actorOf(Props(RegisterProxyClass, this, mmap), name = s"RegisterMiddleman-$k")
    val register = new RoundBasedRegister[T](acceptors, aref, k, contextParam)
    mmap.put(contextParam, register)
    proxyMap.put(k, aref)
    globalRegisterMap.put(aref, mmap)

    register
  }

}

