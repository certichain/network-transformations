package org.protocols.paxos.singledecree

import akka.actor.{Actor, ActorRef}
import org.protocols.paxos.PaxosVocabulary

import scala.collection.immutable.Nil

/**
  * @author Ilya Sergey
  */

trait SingleDecreePaxos[T] {
  // Instantiate messages
  val vocabulary = new PaxosVocabulary[T]

  import vocabulary._

  /**
    * An acceptor class for a Single Decree Paxos
    */
  class Acceptor extends Actor {

    var currentBallot: Ballot = -1
    var chosenValues: List[(Ballot, T)] = Nil

    def lastChosenValue: Option[T] = findMaxBallotAccepted(chosenValues)

    override def receive: Receive = {
      case Phase1A(b, l) =>
        if (b > currentBallot) {
          currentBallot = b
          l ! Phase1B(promise = true, self, lastChosenValue)
        } else {
          /* do nothing */
        }
      case Phase2A(b, l, v) =>
        if (b == currentBallot) {
          // record the value
          chosenValues = (b, v) :: chosenValues
          // we may even ignore this step
          l ! Phase2B(b, self, ack = true)
        } else {
          /* do nothing */
        }

      // Send accepted request
      case QueryAcceptor(sender) =>
        sender ! ValueAcc(self, lastChosenValue)
    }
  }

  /**
    * The proposer class, initiating the agreement procedure
    *
    * @param acceptors a set of acceptors in this instance
    * @param myBallot  fixed ballot number
    */
  class Proposer(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends Actor {

    def proposerPhase1: Receive = {
      case ProposeValue(v) =>
        // Start Paxos round with my givenballot
        for (a <- acceptors) a ! Phase1A(myBallot, self)
        context.become(proposerPhase2(v, Nil))
    }

    def proposerPhase2(v: T, responses: List[(ActorRef, Option[T])]): Receive = {
      case Phase1B(true, a, vOpt) =>
        val newResponses = (a, vOpt) :: responses
        // find maximal group
        val maxGroup = newResponses.groupBy(_._2).toList.map(_._2).maxBy(_.size)
        if (maxGroup.nonEmpty && maxGroup.size > acceptors.size / 2) {
          // found quorum
          val toPropose = maxGroup.head._2 match {
            case Some(w) => w
            case None => v
          }
          val quorum = maxGroup.map(_._1)

          for (a <- quorum) a ! Phase2A(myBallot, self, toPropose)
          context.become(finalStage)
        } else {
          context.become(proposerPhase2(v, newResponses))
        }
    }

    /**
      * Now we only respond to queries about selected values
      */
    def finalStage: Receive = new PartialFunction[Any, Unit] {
      override def isDefinedAt(x: Any): Boolean = false
      override def apply(v1: Any): Unit = {}
    }

    override def receive: Receive = proposerPhase1
  }

  class Learner(val acceptors: Seq[ActorRef]) extends Actor {

    override def receive: Receive = waitForQuery

    def waitForQuery: Receive = {
      case QueryLearner(sender) =>
        for (a <- acceptors) a ! QueryAcceptor(self)
        context.become(respondToQuery(sender, Nil))
    }

    private def respondToQuery(sender: ActorRef,
                               results: List[Option[T]]): Receive = {
      case ValueAcc(a, vOpt) =>
        val newResults = vOpt :: results
        val maxGroup = newResults.groupBy(x => x).toSeq.map(_._2).maxBy(_.size)

        if (maxGroup.nonEmpty && maxGroup.size > acceptors.size / 2) {
          if (maxGroup.head.isEmpty) {
            // No consensus has been reached so far, repeat the procedure from scratch
            self ! QueryLearner(sender)
            context.become(waitForQuery)
          } else {
            // respond to the sender
            sender ! LearnedAgreedValue(maxGroup.head.get, self)
            // TODO: may also cache the result
            context.become(waitForQuery)
          }
        } else {
          context.become(respondToQuery(sender, newResults))
        }
    }
  }

}
