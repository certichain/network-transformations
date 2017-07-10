package org.protocols.paxos.multipaxos

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import org.protocols.paxos.combinators.MessageForSlot
import org.protocols.paxos.{PaxosConfiguration, PaxosFactory}
import org.scalatest._

import scala.concurrent.duration._


/**
  * @author Ilya Sergey
  */

abstract class GenericMultiPaxosTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
    WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem(s"PaxosTests-${hashCode()}"))

  def fact(n: Int): Int = if (n < 1) 1 else n * fact(n - 1)
  override def afterAll() {
    system.terminate()
  }

  val testMap1 = Map(1 -> List("A", "B", "C", "D", "E"),
    2 -> List("Moscow", "Madrid", "London", "Kyiv", "Paris"),
    3 -> List("Cat", "Dog", "Hamster", "Fish", "Turtle"),
    4 -> List("Bread", "Milk", "Kefir", "Sausage", "Beer"),
    5 -> List("UCL", "Imperial", "Kings", "Cambridge", "Oxford"))

  def setupAndTestInstances[A](slotValueMap: Map[Int, List[A]], factory: PaxosFactory[A]): Unit = {
    // TODO generalize this
    val acceptorNum = 7
    val learnerNum = 3
    val proposerNum = 5

    val instance = factory.createPaxosInstance(system, proposerNum, acceptorNum, learnerNum)
    println("")

    proposeValuesForSlots(slotValueMap, instance, factory)

    // Wait for some time
    Thread.sleep(400)
    learnAcceptedValues(slotValueMap, instance, factory)
  }


  private def proposeValuesForSlots[A](slotValueMap: Map[Int, List[A]],
                                       instance: PaxosConfiguration, factory: PaxosFactory[A]) = {

    import factory._

    val threadss: Seq[Seq[Thread]] = for ((slot, values) <- slotValueMap.toSeq) yield {
      // Propose values for this slot
      val perms = values.indices.permutations
      var permInd = perms.next()
      for (i <- 0 until (Math.random() * fact(values.size)).toInt) {
        permInd = perms.next()
      }

      // Inner threads proposing specific values for a fixed slot
      for {(j, i) <- permInd.zip(values.indices)
           v = values(j)
           p = instance.proposers(i)} yield
        new Thread {
          override def run() {
            println(s"Proposing for slot $slot via ${p.path.name} value $v.")
            // Some randomized delays to compensate for ballot distribution
            Thread.sleep((values.size - j + i) * 5, 0)
            p ! MessageForSlot(slot, ProposeValue(v))
          }
        }
    }

    for (t <- threadss.flatten.toSet[Thread]) t.start()

  }


  protected def learnAcceptedValues[A](slotValueMap: Map[Int, List[A]],
                                       instance: PaxosConfiguration,
                                       factory: PaxosFactory[A]): Seq[Seq[MessageForSlot[Any]]] = {
    import factory._
    val learners: Seq[ActorRef] = instance.learners

    val rss =
      for ((slot, _) <- slotValueMap.toSeq.sortBy(_._1)) yield {

        // Learn the results
        println("")
        println(s"[Learning values for slot $slot]")

        for (l <- learners) {
          l ! MessageForSlot(slot, QueryLearner(self))
        }

        // Collect results
        val res = receiveFromLearners(learners).asInstanceOf[Seq[MessageForSlot[PaxosMessage]]]

        for (MessageForSlot(s, LearnedAgreedValue(v, l)) <- res) {
          println(s"Value for a slot $s learnt via ${l.path.name}: $v")
        }

        finalAssertions(learners, factory, res)
        res

      }
    println()
    rss
  }

  protected def finalAssertions[A](learners: Seq[ActorRef], factory: PaxosFactory[A], res: Seq[MessageForSlot[Any]]) = {
    import factory._

    assert(res.size == learners.size, s"heard back from all learners")
    assert(res.forall {
      case MessageForSlot(_, LearnedAgreedValue(v, l)) =>
        v == res.head.msg.asInstanceOf[LearnedAgreedValue].value
      case _ => false
    }, s"All learners should return the same result at the end.")
  }

  protected def receiveFromLearners(learners: Seq[ActorRef]): Seq[AnyRef] = {
    receiveN(learners.size, 5 seconds)
  }
}
