package org.protocols.paxos

import akka.actor.ActorRef

import scala.collection.immutable.Nil

/**
  * @author Ilya Sergey
  */


trait PaxosRoles[T] extends PaxosVocabulary[T] {

  type ToSend = Seq[(ActorRef, PaxosMessage)]
  type Step = PartialFunction[Any, ToSend]

  /**
    * A generic interface for Paxos roles
    */
  sealed trait PaxosRole {
    // Abstract members to be initialized
    protected val self: ActorRef

    def step: Step

    // Adapt the message for the wrapping combinator
    protected def emitOne(a: ActorRef, msg: PaxosMessage) = Seq((a, msg))

    protected def emitMany(as: Seq[ActorRef], f: ActorRef => PaxosMessage): ToSend = as.zip(as.map(a => f(a)))

    protected def emitZero: ToSend = Seq.empty

  }

  /** ***************************************************************/
  /** *********** Specific roles within the Paxos protocol **********/
  /** ***************************************************************/


  /**
    * An acceptor STS
    *
    * @param myStartingBallot Initial ballot to start from
    */
  abstract class AcceptorRole(val myStartingBallot: Ballot = -1) extends PaxosRole {

    var currentBallot: Ballot = myStartingBallot
    var chosenValues: List[(Ballot, T)] = Nil

    //    def getLastChosenValue: Option[T] = findMaxBallotAccepted(chosenValues)

    // This method is _always_ safe to run, as it only reduces the set of Acceptor's behaviors
    def bumpUpBallot(b: Ballot): Unit = {
      if (b > currentBallot) {
        currentBallot = b
      }
    }

    val step: Step = {
      case Phase1A(b, l) =>
        // Using non-strict inequality here for multi-paxos
        if (b >= currentBallot) {
          bumpUpBallot(b)
          emitOne(l, Phase1B(promise = true, self, findMaxBallotAccepted(chosenValues)))
        } else {
          emitZero
        }
      case m@Phase2A(b, l, v, _) =>
        println(s"[Acceptor] processing $m.")
        if (b == currentBallot) {
          // record the value
          chosenValues = (b, v) :: chosenValues
          // we may even ignore this step
          emitOne(l, Phase2B(b, self, ack = true))
        } else {
          emitZero
        }
      // Send accepted request
      case QueryAcceptor(sender) =>
        emitOne(sender, ValueAcc(self, findMaxBallotAccepted(chosenValues).map(_._2)))
    }
  }


  /**
    * A proposer STS
    *
    * @param acceptors specific acceptors
    * @param myBallot  an assigned unique ballot
    */
  abstract class ProposerRole(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends PaxosRole {

    type Responses = List[(ActorRef, Option[(Ballot, T)])]

    private var myValueToPropose: Option[T] = None
    private var canPropose: Boolean = true
    def gotQuorum = myResponses.size > acceptors.size / 2
    private var myResponses: Responses = Nil
    def getResponses: Responses = myResponses

    def unconvincedAcceptors = acceptors.filter(a => !myResponses.exists(_._1 == a))

    def setResponses(rs: Responses) {
      if (myResponses.isEmpty) myResponses = rs
    }

    val step: Step = {
      case ProposeValue(v) =>
        // Only can be used to propose once
        if (myValueToPropose.isEmpty) myValueToPropose = Some(v)
        if (gotQuorum && canPropose) {
          proceedWithQuorum(v)
        } else {
          emitMany(unconvincedAcceptors, _ => Phase1A(myBallot, self))
        }
      case Phase1B(true, a, vOpt) =>
        myResponses = (a, vOpt) :: myResponses.filter(_._1 == a)
        if (gotQuorum && canPropose && myValueToPropose.nonEmpty) {
          proceedWithQuorum(myValueToPropose.get)
        } else {
          emitZero
        }
      case Phase2B(_, _, _) => emitZero
    }

    /**
      * This method is a point-cut to short-circuit the `proposerCollectForQuorum` stage
      *
      * @param v value to be proposed
      * @return messages to be sent to the acceptors
      */
    private def proceedWithQuorum(v: T): ToSend = {
      // It's ellegal to call this function if no quorum is reached
      if (myResponses.size <= acceptors.size / 2) {
        throw new Exception("No quorum has been reached")
      }

      if (!canPropose) {
        throw new Exception("Cannot propose a value any more.")
      }
      canPropose = false

      val (toPropose, mBal, quorumRecipients) = val2a(Some(v))
      assert(toPropose.nonEmpty)
      emitMany(quorumRecipients, _ => Phase2A(myBallot, self, toPropose.get, mBal))
    }


    def val2a(v: Option[T]): (Option[T], Ballot, List[ActorRef]) = {
      // Found quorum, get candidates for the proposal
      val nonEmptyResponses = myResponses.map(_._2).filter(_.nonEmpty)

      // Figure out what to propose along with the last ballot it's been proposed for
      val (mBal, toPropose) = nonEmptyResponses match {
        case Nil =>
          // This is a hook to make this function take myValueToPropose into the account
          // for the case when this one has already proposed
          if (v.isEmpty && myValueToPropose.nonEmpty && canPropose)
            (myBallot, myValueToPropose)
          else (-1, v)
        case rs =>
          val (b, w) = rs.map(_.get).maxBy(_._1) // A highest-ballot proposal
          (b, Some(w))
      }

      (toPropose, mBal, myResponses.map(_._1))
    }
  }


  /**
    * A learner STS
    *
    * @param acceptors acceptors to learn the result from
    */
  abstract class LearnerRole(val acceptors: Seq[ActorRef]) extends PaxosRole {

    def waitForQuery: Step = {
      case QueryLearner(sender) =>
        currentStepFunction = respondToQuery(sender, Nil)
        emitMany(acceptors, _ => QueryAcceptor(self))
      case ValueAcc(_, _) => emitZero // ignore this now, as it's irrelevant
    }

    private def respondToQuery(sender: ActorRef,
                               results: List[Option[T]]): Step = {
      case ValueAcc(a, vOpt) =>
        val newResults = vOpt :: results
        val maxGroup = newResults.groupBy(x => x).toSeq.map(_._2).maxBy(_.size)

        if (maxGroup.nonEmpty && maxGroup.size > acceptors.size / 2) {
          currentStepFunction = waitForQuery
          if (maxGroup.head.isEmpty) {
            // No consensus has been reached so far, repeat the procedure from scratch
            emitOne(self, QueryLearner(sender))
          } else {
            // respond to the sender
            emitOne(sender, LearnedAgreedValue(maxGroup.head.get, self))
          }
        } else {
          currentStepFunction = respondToQuery(sender, newResults)
          emitZero
        }
    }

    private var currentStepFunction: Step = waitForQuery

    def step: Step = currentStepFunction

  }


}
