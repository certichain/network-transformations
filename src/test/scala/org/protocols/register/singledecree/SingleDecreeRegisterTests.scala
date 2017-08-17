package org.protocols.register.singledecree

import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

/**
  * @author Ilya Sergey
  */
class SingleDecreeRegisterTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem(s"SingleDecreeRegisterTests-${hashCode()}"))

  val proposals = Seq("Kyiv", "Madrid", "London", "St Petersburg", "Salamanca", "Dneprodzerzhinsk", "Tel-Aviv")
  val numAcceptors = 7

  s"All participants in a Single Decree Register-Based Paxos" must {
    s"agree on the same accepted value" in {

      val registerProvider = new SingleDecreeRegisterProvider[String](_system, numAcceptors)

      val barrier = new CountDownLatch(proposals.size)

      val ts = for (k <- proposals.indices; v = proposals(k)) yield {
        new Thread() {
          override def run(): Unit = {
            Thread.sleep(((200 + 50 * k) * Math.random()).toInt)
            println(s"Proposing with ballot [$k] value [$v].")
            val r = registerProvider.getSingleServedRegister(k)
            r.propose(v)
            barrier.countDown()
          }
        }
      }

      // Run all tests
      println("Starting parallel proposals and awaiting for the results")
      for (t <- ts) {
        t.start()
      }
      barrier.await(10, TimeUnit.SECONDS)


      println
      print("Collecting results... ")
      // Collecting results
      val results =
        for {k <- proposals.indices
             r = registerProvider.getSingleServedRegister(k)}
          yield {
            var res = r.read()._2
            // Wait for result
            while (res.isEmpty) {
              res = r.read()._2
            }
            res
          }
      println(s"Done.")

      print(s"Asserting that values for all slots are decided... ")
      assert(results.forall(_.nonEmpty), results)
      println(s"OK!")

      assert(results.nonEmpty)
      val v0 = results.head.get

      print(s"Asserting that all values are same... ")
      for (vOpt <- results) {
        assert(vOpt.get == v0, s"Should be $v0, but it's ${vOpt.get} in\n$results")
      }
      println(s"OK!")

      _system.terminate()
      println(s"Values: [${results.map(_.get).mkString(", ")}]")
      println("All good")
      println
    }
  }
}
