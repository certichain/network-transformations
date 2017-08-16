package org.protocols.register.multipaxos

import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.protocols.register.RoundRegisterProvider
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

/**
  * @author Ilya Sergey
  */

abstract class GenericRegisterBasedMultiPaxosTests(val _system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with
        WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem(s"ReplicatedRegisterTests-${hashCode()}"))

  def getRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String]

  val testMap1 = Map[Int, List[String]](1 -> List("A", "B", "C", "D", "E"),
    2 -> List("Moscow", "Madrid", "London", "Kyiv", "Paris"),
    3 -> List("Cat", "Dog", "Hamster", "Fish", "Turtle"),
    4 -> List("Bread", "Milk", "Kefir", "Sausage", "Beer"),
    5 -> List("UCL", "Imperial", "Kings", "Cambridge", "Oxford"))


  // Main test
  s"All participants in a Register-Based MultiPaxos" must {
    s"agree on the accepted values for each slot" in {

      val numAcceptors = 7
      val registerProvider = getRegisterProvider(numAcceptors)

      // Number of proposals
      val numProp = testMap1.values.map(_.size).max

      val barrier = new CountDownLatch(numProp)

      val ts = for (k <- 0 until numProp) yield {
        new Thread() {
          override def run(): Unit = {
            for (s <- testMap1.keys) {
              val r = registerProvider.getSingleServedRegister(k, s)
              val v = testMap1(s)(k)
              println(s"Proposing for slot [$s] with ballot [$k] value [$v].")
              Thread.sleep((800 * Math.random()).toInt)
              r.propose(v)
            }
            // Done with proposing
            barrier.countDown()
          }
        }
      }

      // Run all tests
      println("Starting parallel proposals and awaiting for the results...")
      println
      for (t <- ts) {
        t.start()
      }
      barrier.await(20, TimeUnit.SECONDS)

      println
      print("Collecting results... ")
      // Collecting results
      val results = (for (s <- testMap1.keys) yield {
        val rs = for (k <- 0 until numProp;
                      r = registerProvider.getSingleServedRegister(k, s))
          yield r.read()._2
        s -> rs.toList
      }).toMap
      println(s"Done.")


      print(s"Asserting that values for all slots are decided... ")
      for (s <- results.keySet.toSeq.sorted;
           rs = results(s)) {
        assert(rs.size == numProp, s"Not enough results (${rs.size}, but should be $numProp) in $rs")
        assert(rs.forall(_.nonEmpty), s"All results for slot $s should be empty: $rs")
      }
      println(s"OK!")

      print(s"Asserting that all values are same... ")
      println(s"OK!")
      for (s <- results.keySet.toSeq.sorted;
           rs = results(s)) {
        for (vOpt <- rs) {
          val v0 = rs.head.get
          assert(vOpt.get == v0, s"Should be $v0, but it's ${vOpt.get} in\n$results")
        }
      }

      println
      println(s"Results per slot:")
      for (s <- results.keySet.toSeq.sorted;
           rs = results(s)) {
        println(s"$s -> [${rs.map(_.get).mkString(", ")}]")
      }
      println
      println("All good")
      println

      _system.terminate()
    }
  }
}
