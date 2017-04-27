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

    protected def initStepHandler: Step

    def step: Step = currentStepFunction

    // Adapt the message for the wrapping combinator
    protected def emitOne(a: ActorRef, msg: PaxosMessage) = Seq((a, msg))

    protected def emitMany(as: Seq[ActorRef], f: ActorRef => PaxosMessage): ToSend = as.zip(as.map(a => f(a)))

    protected def emitZero: ToSend = Seq.empty

    protected def become(r: Step) {
      currentStepFunction = r
    }

    private var currentStepFunction: Step = initStepHandler
  }

  /** ***************************************************************/
  /** *********** Specific roles within the Paxos protocol **********/
  /** ***************************************************************/


  ////////////////////////////////////////////////////////////////////
  //////////////////////       Acceptor      /////////////////////////
  ////////////////////////////////////////////////////////////////////

  abstract class AcceptorRole(val myStartingBallot: Int = -1) extends PaxosRole {

    var currentBallot: Ballot = myStartingBallot
    var chosenValues: List[(Ballot, T)] = Nil

    // This method is _always_ safe to run, as it only reduces the set of Acceptor's behaviors
    def bumpUpBallot(b: Ballot): Unit = {
      if (b > currentBallot) {
        currentBallot = b
      }
    }

    def getLastChosenValue: Option[T] = findMaxBallotAccepted(chosenValues)

    final override def initStepHandler: Step = {
      case Phase1A(b, l) =>
        // Using non-strict inequality here for multi-paxos
        if (b >= currentBallot) {
          bumpUpBallot(b)
          emitOne(l, Phase1B(promise = true, self, findMaxBallotAccepted(chosenValues)))
        } else {
          emitZero
        }
      case Phase2A(b, l, v) =>
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
        emitOne(sender, ValueAcc(self, findMaxBallotAccepted(chosenValues)))
    }
  }


  ////////////////////////////////////////////////////////////////////
  //////////////////////       Proposer      /////////////////////////
  ////////////////////////////////////////////////////////////////////

  abstract class ProposerRole(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends PaxosRole {

    final override def initStepHandler: Step = proposerPhase1

    def proposerPhase1: Step = {
      case ProposeValue(v) =>
        // Start Paxos round with my given ballot
        become(proposerPhase2(v, Nil))
        emitMany(acceptors, _ => Phase1A(myBallot, self))
    }

    def proposerPhase2(v: T, responses: List[(ActorRef, Option[T])]): Step = {
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
          become(finalStage)
          emitMany(quorum, _ => Phase2A(myBallot, self, toPropose))
          // Enter the final stage
        } else {
          become(proposerPhase2(v, newResponses))
          emitZero
        }
    }

    // Starting now we only respond to queries about selected values
    def finalStage: Step = new PartialFunction[Any, ToSend] {
      override def isDefinedAt(x: Any): Boolean = false
      override def apply(v1: Any): ToSend = emitZero
    }

  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////       Learner       /////////////////////////
  ////////////////////////////////////////////////////////////////////

  abstract class LearnerRole(val acceptors: Seq[ActorRef]) extends PaxosRole {

    final override def initStepHandler: Step = waitForQuery

    def waitForQuery: Step = {
      case QueryLearner(sender) =>
        become(respondToQuery(sender, Nil))
        emitMany(acceptors, _ => QueryAcceptor(self))
      case ValueAcc(_, _) => emitZero // ignore this now, as it's irrelevant
    }

    private def respondToQuery(sender: ActorRef,
                               results: List[Option[T]]): Step = {
      case ValueAcc(a, vOpt) =>
        val newResults = vOpt :: results
        val maxGroup = newResults.groupBy(x => x).toSeq.map(_._2).maxBy(_.size)

        if (maxGroup.nonEmpty && maxGroup.size > acceptors.size / 2) {
          become(waitForQuery)
          if (maxGroup.head.isEmpty) {
            // No consensus has been reached so far, repeat the procedure from scratch
            emitOne(self, QueryLearner(sender))
          } else {
            // respond to the sender
            emitOne(sender, LearnedAgreedValue(maxGroup.head.get, self))
          }
        } else {
          become(respondToQuery(sender, newResults))
          emitZero
        }
    }
  }


}
