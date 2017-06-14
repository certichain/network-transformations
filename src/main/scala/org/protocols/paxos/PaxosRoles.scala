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

  type AcceptorRole = AcceptorRoleImpl
  type ProposerRole = ProposerRoleImpl
  type LearnerRole = LearnerRoleImpl

  /**
    * Use this to create new instances of roles, not the constructors!
    */
  def createAcceptor(slf: ActorRef): AcceptorRole = new AcceptorRoleImpl() {
    val self = slf
  }

  def createProposer(acceptors: Seq[ActorRef], myBallot: Ballot, slf: ActorRef): ProposerRole =
    new ProposerRoleImpl(acceptors, myBallot) {
      val self = slf
    }

  def createLearner(acceptors: Seq[ActorRef], slf: ActorRef): LearnerRole =
    new LearnerRoleImpl(acceptors) {
      val self = slf
    }

  /**
    * An acceptor STS
    *
    * @param myStartingBallot Initial ballot to start from
    */
  abstract class AcceptorRoleImpl(val myStartingBallot: Ballot = -1) extends PaxosRole {

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
  abstract class ProposerRoleImpl(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends PaxosRole {

    import collection.mutable.{Map => MMap}

    private var myValueToPropose: Option[T] = None
    private var notYetProposed: Boolean = true
    private val myResponses: MMap[ActorRef, Option[(Ballot, T)]] = MMap.empty
    private def gotQuorum = myResponses.size > acceptors.size / 2
    private def unconvincedAcceptors = acceptors.filter(a => !myResponses.isDefinedAt(a))

    /**
      * Three public methods
      */
    def hasProposed = !notYetProposed

    // TODO: prove that this is a monotone optimization
    def addResponses(rs: Set[(ActorRef, Option[(Ballot, T)])]) {
      for ((a, r) <- rs if !myResponses.isDefinedAt(a)) myResponses.update(a, r)
    }

    def val2a: (Option[T], Ballot, List[ActorRef]) = {
      // It's illegal to call this function if no quorum is reached
      if (myResponses.size <= acceptors.size / 2) {
        throw new Exception("No quorum has been reached")
      }

      // Found quorum, get candidates for the proposal
      val nonEmptyResponses = myResponses.values.filter(_.nonEmpty)

      // Figure out what to propose along with the last ballot it's been proposed for
      val (mBal, toPropose) = nonEmptyResponses match {
        case Nil => (myBallot, myValueToPropose)
        // It's important to distinguish between querying for
        // stoppable purposes and for committing by myself
        case rs =>
          val (c, w) = rs.map(_.get).maxBy(_._1) // A highest-ballot proposal
          (c, Some(w))
      }

      (toPropose, mBal, myResponses.keySet.toList)
    }

    def step: Step = {
      case ProposeValue(v) =>
        // Only can be used to propose once
        if (myValueToPropose.isEmpty) myValueToPropose = Some(v)
        if (gotQuorum && notYetProposed) {
          proceedWithQuorum()
        } else {
          emitMany(unconvincedAcceptors, _ => Phase1A(myBallot, self))
        }
      case Phase1B(true, a, vOpt) =>
        addResponses(Set((a, vOpt)))
        if (gotQuorum && notYetProposed && myValueToPropose.nonEmpty) {
          proceedWithQuorum()
        } else {
          emitZero
        }
      case Phase2B(_, _, _) => emitZero
    }


    /**
      * Essentially a "commit" method for the value to be proposed
      *
      * @return messages to be sent to the acceptors
      */
    private def proceedWithQuorum(): ToSend = {
      if (myValueToPropose.isEmpty) {
        throw new Exception("No value to propose")
      }
      if (!notYetProposed) {
        throw new Exception("Cannot propose a value any more.")
      }

      val (toPropose, mBal, quorumRecipients) = val2a
      assert(toPropose.nonEmpty)

      notYetProposed = false
      emitMany(quorumRecipients, _ => Phase2A(myBallot, self, toPropose.get, mBal))
    }
  }


  /**
    * A learner STS
    *
    * @param acceptors acceptors to learn the result from
    */
  abstract class LearnerRoleImpl(val acceptors: Seq[ActorRef]) extends PaxosRole {

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
