package org.protocols.paxos.multipaxos

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._

import scala.concurrent.duration._


/**
  * @author Ilya Sergey
  */

class MultiPaxosTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("SingleDecreePaxosTests"))

  override def afterAll() {
    system.shutdown()
  }

  s"All learners" must {
    s"agree on the same non-taken value" in {
      // A map from slots to values
      val map = Map(1 -> List("A", "B", "C", "D", "E"),
        2 -> List("Moscow", "Madrid", "London", "Kyiv", "Paris"),
        3 -> List("Cat", "Dog", "Hamster", "Fish", "Turtle"))
      setupAndTestInstances(map)
    }

  }

  def fact(n: Int): Int = if (n < 1) 1 else n * fact(n - 1)

  def setupAndTestInstances[A](slotValueMap: Map[Int, List[A]]): Unit = {

    // TODO generalize this
    val acceptorNum = 5
    val learnerNum = 5
    val proposerNum = 5

    val paxosFactory = new MultiPaxosFactory[A]
    import paxosFactory._

    val instance = paxosFactory.createPaxosInstance(system, proposerNum, acceptorNum, learnerNum)

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
            p ! MessageWithSlot(slot, ProposeValue(v))
          }
        }

      println("")
      println(s"[Proposing values for slot $slot]")
      for (r <- rs) r.start()
    }


    // Wait for some time
    Thread.sleep(800)
    val learners = instance.learners

    for ((slot, _) <- slotValueMap) {

      // Learn the results
      println("")
      println(s"[Learning values for slot $slot]")

      for (l <- learners) {
        l ! MessageWithSlot(slot, QueryLearner(self))
      }

      // Collect results
      val res = receiveN(learners.size, 5 seconds).asInstanceOf[Seq[MessageWithSlot]]

      for (MessageWithSlot(s, LearnedAgreedValue(v, l)) <- res) {
        println(s"Value for a slot $s from learner [$l]: $v")
      }

      assert(res.size == learners.size, s"heard back from all learners")
      assert(res.forall { case MessageWithSlot(_, LearnedAgreedValue(v, l)) =>
        v == res.head.msg.asInstanceOf[LearnedAgreedValue].value
      }, s"All learners should return the same result at the end.")

    }

  }

}
