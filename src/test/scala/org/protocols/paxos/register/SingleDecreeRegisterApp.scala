package org.protocols.paxos.register

import akka.actor.ActorSystem

/**
  * @author Ilya Sergey
  */
object SingleDecreeRegisterApp extends App {

  // create the system and actor
  val system = ActorSystem("RegisterSystem")
  private val numAcceptors = 7
  val registerProvider = new SingleDecreeRegisterProvider[String](system, numAcceptors)

  val register1 = registerProvider.getRegisterToPropose
  val register2 = registerProvider.getRegisterToPropose
  val register3 = registerProvider.getRegisterToPropose

  val v1 = "Leuven"
  val v2 = "Madrid"
  val v3 = "London"

  println(s"Proposing: $v1")
  val w1 = register1.propose(v1)
  assert(w1.nonEmpty)
  assert(w1.get == "Leuven")

  println(s"Proposing: $v2")
  val w2 = register2.propose(v2)
  println(s"Proposing: $v3")
  val w3 = register3.propose(v3)

  assert(w2.get == w1.get, s"Should be ${w1.get}, but it's ${w2.get}")
  assert(w3.get == w1.get, s"Should be ${w1.get}, but it's ${w3.get}")

  system.terminate()

}
