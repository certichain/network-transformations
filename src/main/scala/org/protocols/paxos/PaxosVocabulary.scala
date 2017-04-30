package org.protocols.paxos

import akka.actor.ActorRef

import scala.collection.immutable.Nil

/**
  * Common vocabulary for Paxos-like protocols
  *
  * @author Ilya Sergey
  */
trait PaxosVocabulary[T] {

  type Ballot = Int

  sealed trait PaxosMessage

  case class Phase1A(ballot: Ballot,
                     leader: ActorRef) extends PaxosMessage

  case class Phase1B(promise: Boolean,
                     acceptor: ActorRef,
                     valueOpt: Option[(Ballot, T)]) extends PaxosMessage

  case class Phase2A(myBallot: Ballot,
                     proposer: ActorRef,
                     data: T,
                     mbal: Ballot) extends PaxosMessage

  case class Phase2B(acceptedBallot: Ballot,
                     acceptor: ActorRef,
                     ack: Boolean) extends PaxosMessage


  // Administrative messages for initializing the consensus
  case class ProposeValue(value: T) extends PaxosMessage

  // Administrative messages for querying
  case class QueryAcceptor(sender: ActorRef) extends PaxosMessage
  case class ValueAcc(acc: ActorRef, valueOpt: Option[T]) extends PaxosMessage

  case class QueryLearner(sender: ActorRef) extends PaxosMessage
  case class LearnedAgreedValue(value: T, learner: ActorRef) extends PaxosMessage

  // Some library functions
  def findMaxBallotAccepted(chosenValues: List[(Ballot, T)]): Option[(Ballot, T)] =
    chosenValues match {
      case Nil => None
      case x => Some(x.maxBy(_._1))
    }

}
