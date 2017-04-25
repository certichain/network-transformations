package org.protocols.twophase

import akka.actor._
import scala.collection.mutable.{Map => MMap}

/**
  * @author Ilya Sergey
  */

// Messages of the protocol
sealed trait TwoPhaseMessage {
  val round: Int
}

case class StartPoll[A](round: Int, nodes: Set[ActorRef], value: A) extends TwoPhaseMessage
case class Query[A](round: Int, coord: ActorRef, value: A) extends TwoPhaseMessage
case class Yes(round: Int, node: ActorRef) extends TwoPhaseMessage
case class No(round: Int, node: ActorRef) extends TwoPhaseMessage
case class Commit(round: Int, coord: ActorRef) extends TwoPhaseMessage
case class Ack(round: Int, node: ActorRef) extends TwoPhaseMessage

// Testing messages
case class Ask(round: Int, node: ActorRef) extends TwoPhaseMessage
case class Resp[A](round: Int, node: ActorRef, v: Option[A]) extends TwoPhaseMessage
case class Dunno(round: Int, node: ActorRef) extends TwoPhaseMessage

sealed abstract class TwoPhaseActor(val startingRound: Int) extends Actor

class TwoPhaseCoordinator[A](val round: Int) extends TwoPhaseActor(round) {

  private val yesVotes: MMap[Int, Set[ActorRef]] = MMap.empty.withDefaultValue(Set.empty)
  private val acks: MMap[Int, Set[ActorRef]] = MMap.empty.withDefaultValue(Set.empty)

  override def receive: Receive = init(startingRound, None)

  def init(round: Int, v: Option[A]): Receive = {
    case StartPoll(r, nodes, value) if r >= round =>
      for (n <- nodes) n ! Query(r, context.self, value)

      // Dirty type cast!
      context.become(collectYes(round, nodes, value.asInstanceOf[A]))

    case Ask(r, n) =>
      if (r == round - 1) {
        n ! Resp(r, context.self, v)
      } else {
        n ! Dunno(r, context.self)
      }
  }

  def collectYes(round: Int, nodes: Set[ActorRef], value: A): Receive = {
    case Yes(r, n) if r == round && nodes.contains(n) =>
      yesVotes(r) = yesVotes(r) + n
      if (yesVotes(r) == nodes) {
        for (n <- nodes) n ! Commit(round, context.self)
        context.become(collectAcks(round, nodes, value))
      }

    case No(r, n) if r == round && nodes.contains(n) =>
      context.become(init(r + 1, None))
  }

  def collectAcks(round: Int, nodes: Set[ActorRef], value: A): Receive = {
    case Ack(r, n) if r == round && nodes.contains(n) =>
      acks(r) = acks(r) + n
      if (acks(r) == nodes) {
        context.become(init(round + 1, Some(value)))
      }
    // otherwise keep waiting
  }
}


class TwoPhaseNode[A](round: Int, cond: A => Boolean) extends TwoPhaseActor(round) {

  override def receive = init(round)

  def init(round: Int): Receive = {
    case Query(r, coord, v) if r >= round =>
      // Oh, this is nasty
      if (cond(v.asInstanceOf[A])) {
        coord ! Yes(r, context.self)
      } else {
        coord ! No(r, context.self)
      }
      context.become(ack(round, coord))
  }

  def ack(round: Int, coord: ActorRef): Receive = {
    case Commit(r, c) if r == round && c == coord =>
      coord ! Ack(round, context.self)
      context.become(init(round + 1))
  }
}




