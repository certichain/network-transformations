package org.protocols.twophase

import akka.actor._
import akka.testkit._
import org.scalatest._
import scala.concurrent.duration._

/**
 * @author ilya
 */
class TwoPhaseCommitTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("Consensus23Tests"))

  override def afterAll() {
    system.terminate()
  }

  s"A decision" must {
    s"be taken if all nodes agree on the same value (Some(5))" in {
      setupAndTestRound(0, 7, 5, (_: Int) => (Int) => true, Some(5))
    }

    s"not be taken if at least one node disagrees" in {
      setupAndTestRound(1, 6, 5, (z: Int) => (Int) => z != 3, None)
    }

  }


  def setupAndTestRound[A](round: Int, numNodes: Int,
                           value: Int,
                           cond: Int => Int => Boolean,
                           expectedResult: Option[A]): Unit = {

    // start coordinator
    val c = system.actorOf(Props(classOf[TwoPhaseCoordinator[Int]], round), name = s"coordinator-starting-with-round-$round")

    // start nodes
    val nodes = generatePeons(round, numNodes, cond).toSet

    // Start poll for this round
    c ! StartPoll(round, nodes, value)

    Thread.sleep(200)

    // Ask for the result
    c ! Ask(round, self)

    val res = receiveOne(30 seconds)

    res match {
      case Resp(r, n, vopt) =>
        println(s"Round: $round, Number of nodes: $numNodes")
        assert(r == round, s"Wrong round: $r received when $round expected")
        println(s"At the round $round, the consensus result is $vopt")
        assert(vopt == expectedResult, s"the result of consensus should be $expectedResult")
    }

    println()
  }

  def generatePeons[A](round: Int, n: Int, cond: Int => Int => Boolean): IndexedSeq[ActorRef] =
    for (i <- 0 until n) yield {
      system.actorOf(Props(classOf[TwoPhaseNode[Int]], round, cond(i)), name = s"node-$i-starting-with-round-$round")
    }
}

