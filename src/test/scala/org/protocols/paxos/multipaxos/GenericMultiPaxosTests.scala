package org.protocols.paxos.multipaxos

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.protocols.paxos.combinators.MessageForSlot
import org.protocols.paxos.multipaxos.bundling.AcceptorBundlingMultiPaxosFactory
import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxosFactory
import org.protocols.paxos.{PaxosConfiguration, PaxosFactory}
import org.scalatest._

import scala.concurrent.duration._


/**
  * @author Ilya Sergey
  */

abstract class GenericMultiPaxosTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("SingleDecreePaxosTests"))

  def fact(n: Int): Int = if (n < 1) 1 else n * fact(n - 1)
  override def afterAll() {
    system.shutdown()
  }

  val testMap1 = Map(1 -> List("A", "B", "C", "D", "E"),
    2 -> List("Moscow", "Madrid", "London", "Kyiv", "Paris"),
    3 -> List("Cat", "Dog", "Hamster", "Fish", "Turtle"))

  def setupAndTestInstances[A](slotValueMap: Map[Int, List[A]], factory: PaxosFactory[A]): Unit = {
    // TODO generalize this
    val acceptorNum = 5
    val learnerNum = 5
    val proposerNum = 5

    val instance = factory.createPaxosInstance(system, proposerNum, acceptorNum, learnerNum)
    proposeValuesForSlots(slotValueMap, instance, factory)

    // Wait for some time
    Thread.sleep(400)
    learnAcceptedValues(slotValueMap, instance, factory)
  }


  private def proposeValuesForSlots[A](slotValueMap: Map[Int, List[A]],
                                       instance: PaxosConfiguration, factory: PaxosFactory[A]) = {
    import factory._
    for ((slot, values) <- slotValueMap) {
      // Propose values for this slot
      val perms = values.indices.permutations
      var permInd = perms.next()
      for (i <- 0 until (Math.random() * fact(values.size)).toInt) {
        permInd = perms.next()
      }

      val rs = for {(j, i) <- permInd.zip(values.indices)
                    v = values(j)
                    p = instance.proposers(i)} yield
        new Thread {
          override def run() {
            println(s"Proposing $v via $p.")
            // Some randomized delays to compensate for ballot distribution
            Thread.sleep((values.size - j + i) * 5, 0)
            p ! MessageForSlot(slot, ProposeValue(v))
          }
        }

      println("")
      println(s"[Proposing values for slot $slot]")
      for (r <- rs) r.start()
    }
  }


  private def learnAcceptedValues[A](slotValueMap: Map[Int, List[A]],
                                     instance: PaxosConfiguration, factory: PaxosFactory[A]) = {
    import factory._
    val learners = instance.learners

    for ((slot, _) <- slotValueMap) {

      // Learn the results
      println("")
      println(s"[Learning values for slot $slot]")

      for (l <- learners) {
        l ! MessageForSlot(slot, QueryLearner(self))
      }

      // Collect results
      val res = receiveN(learners.size, 5 seconds).asInstanceOf[Seq[MessageForSlot[PaxosMessage]]]

      for (MessageForSlot(s, LearnedAgreedValue(v, l)) <- res) {
        println(s"Value for a slot $s from learner [$l]: $v")
      }

      assert(res.size == learners.size, s"heard back from all learners")
      assert(res.forall { case MessageForSlot(_, LearnedAgreedValue(v, l)) =>
        v == res.head.msg.asInstanceOf[LearnedAgreedValue].value
      }, s"All learners should return the same result at the end.")

    }
  }
}
