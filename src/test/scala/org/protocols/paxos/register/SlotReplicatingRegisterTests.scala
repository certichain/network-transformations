//package org.protocols.paxos.register
//
//import akka.actor.ActorSystem
//import akka.testkit.{ImplicitSender, TestKit}
//import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
//
///**
//  * @author Ilya Sergey
//  */
//
//class SlotReplicatingRegisterTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
//    WordSpecLike with MustMatchers with BeforeAndAfterAll {
//
//  def this() = this(ActorSystem(s"RegisterBasedPaxosTests-${hashCode()}"))
//
//  val testMap1 = Map(1 -> List("A", "B", "C", "D", "E"),
//    2 -> List("Moscow", "Madrid", "London", "Kyiv", "Paris"),
//    3 -> List("Cat", "Dog", "Hamster", "Fish", "Turtle"),
//    4 -> List("Bread", "Milk", "Kefir", "Sausage", "Beer"),
//    5 -> List("UCL", "Imperial", "Kings", "Cambridge", "Oxford"))
//
//
//  s"All participants in a Single Decree Register-Based Paxos" must {
//    s"agree on the same accepted value" in {
//
//      val numAcceptors = 7
//      val registerProvider = new SingleDecreeRegisterProvider[String](_system, numAcceptors)
//
//      val v1 = "Kyiv"
//      val v2 = "Madrid"
//      val v3 = "London"
//      val vs = Seq(v1, v2, v3)
//
//      val rts = for (k <- vs.indices; v = vs(k)) yield {
//        val t = new Thread() {
//          override def run(): Unit = {
//            val r = registerProvider.getSingleServedRegister(k)
//            Thread.sleep((800 * Math.random()).toInt)
//            println(s"Proposing: $v")
//            r.propose(v)
//          }
//        }
//        t
//      }
//
//      val registers = for ((r, t) <- rts) yield {
//        t.start()
//        r
//      }
//
//      println("About to collect results")
//
//      // Spin while waiting for the resulst
//      Thread.sleep(1500)
//      val values = for (r <- registers) yield r.read()._2
//
//      println(s"Asserting that all values are set")
//      assert(values.forall(_.nonEmpty), values)
//
//      assert(values.nonEmpty)
//      val v0 = values.head.get
//
//      println(s"Asserting that all values are the same")
//      for (vOpt <- values) {
//        assert(vOpt.get == v0, s"Should be $v0, but it's ${vOpt.get} in\n$values")
//      }
//
//      _system.terminate()
//      println(s"Values: $values")
//      println("All good")
//    }
//  }
//}
