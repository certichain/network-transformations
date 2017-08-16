package org.protocols.paxos.register

import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

/**
  * @author Ilya Sergey
  */
class SingleDecreeRegisterTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem(s"RegisterBasedPaxosTests-${hashCode()}"))

  s"All participants in a Single Decree Register-Based Paxos" must {
    s"agree on the same accepted value" in {

      val numAcceptors = 7
      val registerProvider = new SingleDecreeRegisterProvider[String](_system, numAcceptors)

      val v1 = "Kyiv"
      val v2 = "Madrid"
      val v3 = "London"
      val vs = Seq(v1, v2, v3)
      val barrier = new CountDownLatch(vs.size)

      val ts = for (k <- vs.indices; v = vs(k)) yield {
        new Thread() {
          override def run(): Unit = {
            Thread.sleep((800 * Math.random()).toInt)
            println(s"Proposing: $v")
            val r = registerProvider.getSingleServedRegister(k)
            r.propose(v)
            barrier.countDown()
          }
        }
      }

      // Run all tests
      println("Starting parallel proposals")
      for (t <- ts) {
        t.start()
      }
      barrier.await(10, TimeUnit.SECONDS)


      //      Thread.sleep(5000)
      println("Collecting results")
      // Collecting results
      val results =
        for {k <- vs.indices
             r = registerProvider.getSingleServedRegister(k)}
          yield r.read()._2

      println(s"Asserting that all values are set")
      assert(results.forall(_.nonEmpty), results)

      assert(results.nonEmpty)
      val v0 = results.head.get

      println(s"Asserting that all values are the same")
      for (vOpt <- results) {
        assert(vOpt.get == v0, s"Should be $v0, but it's ${vOpt.get} in\n$results")
      }

      _system.terminate()
      println(s"Values: $results")
      println("All good")
    }
  }
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
