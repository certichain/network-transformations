package org.protocols.paxos.singledecree

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._

import scala.concurrent.duration._


/**
  * @author Ilya Sergey
  */

class SingleDecreePaxosTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("SingleDecreePaxosTests"))

  override def afterAll() {
    system.shutdown()
  }

  // TODO 2: Test the Paxos functionality

  s"All learners" must {
    s"agree on the same non-taken value" in {
      val values = List("A", "B", "C", "D", "E")
      setupAndTestInstances(values)
    }

  }

  def fact(n: Int): Int = if (n < 1) 1 else n * fact(n - 1)

  def setupAndTestInstances[A](values: List[A]): Unit = {

    val acceptorNum = 5
    val learnerNum = values.length
    val proposerNum = values.length

    val paxosFactory = new SingleDecreePaxosFactory[A]
    // Import message types
    import paxosFactory.vocabulary._


    val instance = paxosFactory.createPaxosSimplePaxosInstance(system, proposerNum, acceptorNum, learnerNum)

    // Propose values
    val perms = values.indices.permutations
    var permInd = perms.next()
    for (i <- 0 until (Math.random() * fact(values.size)).toInt) {
      permInd = perms.next()
    }
    println(permInd)

    println("[Proposing values]")
    val rs = for {(j, i) <- permInd.zip(values.indices)
                  v = values(j)
                  p = instance.proposers(i)} yield
      new Thread {
        override def run() {
          println(s"Proposing $v via $p.")
          // Some randomized delays to compensate for ballot distribution
          Thread.sleep((values.size - j + i) * 5, 0)
          p ! ProposeValue(v)
        }
      }
    for (r <- rs) r.start()


    // Wait for some time
    Thread.sleep(800)

    // Learn the results
    println("")
    println("[Learning values]")

    val learners = instance.learners
    for (l <- learners) {
      l ! QueryLearner(self)
    }

    // Collect results
    val res = receiveN(learners.size, 5 seconds).asInstanceOf[Seq[LearnedAgreedValue]]

    for (LearnedAgreedValue(v, l) <- res) {
      println(s"Value from learner [$l]: $v")
    }

    assert(res.size == learners.size, s"heard back from all learners")
    assert(res.forall { case LearnedAgreedValue(v, l) => v == res.head.value },
      s"All learners should return the same result at the end.")

    instance.killAll()
  }

}
