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

  val v1 = "Kyiv"
  val v2 = "Madrid"
  val v3 = "London"

  val rts = for (v <- Seq(v1, v2, v3)) yield {
    val r = registerProvider.getSingleServedRegister
    val t = new Thread() {
      override def run(): Unit = {
        Thread.sleep((800 * Math.random()).toInt)
        println(s"Proposing: $v")
        r.propose(v)
      }
    }
    (r, t)
  }

  val registers = for ((r, t) <- rts) yield {
    t.start()
    r
  }

  println("About to collect results")
  Thread.sleep(2000)

  val values = for (r <- registers) yield r.read()._2

  println(s"Asserting that all values are set")
  assert(values.forall(_.nonEmpty), values)

  assert(values.nonEmpty)
  val v0 = values.head.get

  println(s"Asserting that all values are the same")
  for (vOpt <- values) {
    assert(vOpt.get == v0, s"Should be $v0, but it's ${vOpt.get} in\n$values")
  }


  system.terminate()
  println(s"Values: $values")
  println("All good")

}


//  val register1 = registerProvider.getSingleServedRegister
//  val register2 = registerProvider.getSingleServedRegister
//  val register3 = registerProvider.getSingleServedRegister
//  println(s"Proposing: $v1")
//  val w1 = register1.propose(v1)
//  assert(w1.nonEmpty)
//  assert(w1.get == "Leuven")
//
//  println(s"Proposing: $v2")
//  val w2 = register2.propose(v2)
//  println(s"Proposing: $v3")
//  val w3 = register3.propose(v3)
//
//  println("Check that subsequent results agree with the first one.")
//  assert(w2.get == w1.get, s"Should be ${w1.get}, but it's ${w2.get}")
//  assert(w3.get == w1.get, s"Should be ${w1.get}, but it's ${w3.get}")
//
//  println("Checking reads.")
//  val z1 = register1.read()._2.get
//  val z2 = register2.read()._2.get
//  val z3 = register2.read()._2.get
//
//  // Sanity check: reading
//  assert(z1 == w1.get, s"Should be ${w1.get}, but it's $z1")
//  assert(z2 == w1.get, s"Should be ${w1.get}, but it's $z2")
//  assert(z3 == w1.get, s"Should be ${w1.get}, but it's $z3")
