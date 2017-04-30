package org.protocols.paxos.singledecree

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.protocols.paxos.singledecree.monolithic.MonolithicSDPaxosFactory
import org.protocols.paxos.{PaxosConfiguration, PaxosFactory}
import org.scalatest._

import scala.concurrent.duration._


/**
  * @author Ilya Sergey
  */

abstract class AbstractSingleDecreePaxosTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("SingleDecreePaxosTests"))

  override def afterAll() {
    system.shutdown()
  }

  protected def setupAndTestInstances[A](values: List[A], factory: PaxosFactory[A], acceptorNum : Int): Unit = {

    val learnerNum = values.length
    val proposerNum = values.length


    val instance = factory.createPaxosInstance(system, proposerNum, acceptorNum, learnerNum)
    val leaders: Seq[ActorRef] = instance.leaders

    // Propose values
    proposeValues(values, factory, leaders)

    // Wait for some time
    Thread.sleep(800)

    // Learn the results
    val learners = instance.learners
    learnAcceptedValues(learners, factory)

    instance.killAll()
    afterAll()
  }

  private def proposeValues[A](values: List[A], factory: PaxosFactory[A], proposers: Seq[ActorRef]): Unit = {
    import factory._
    val perms = values.indices.permutations
    var permInd = perms.next()
    for (i <- 0 until (Math.random() * fact(values.size)).toInt) {
      permInd = perms.next()
    }
    println(permInd)

    println("[Proposing values]")
    val rs = for {(j, i) <- permInd.zip(values.indices)
                  v = values(j)
                  p = proposers(i)} yield
      new Thread {
        override def run() {
          println(s"Proposing $v via $p.")
          // Some randomized delays to compensate for ballot distribution
          Thread.sleep((values.size - j + i) * 5, 0)
          p ! ProposeValue(v)
        }
      }
    for (r <- rs) r.start()

  }

  private def learnAcceptedValues[A](learners: Seq[ActorRef], factory: PaxosFactory[A]) = {
    import factory._
    println("")
    println("[Learning values]")

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
  }

  private def fact(n: Int): Int = if (n < 1) 1 else n * fact(n - 1)

}




