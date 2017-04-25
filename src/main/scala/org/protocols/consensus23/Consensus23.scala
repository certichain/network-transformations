package org.protocols.consensus23

import akka.actor._
import scala.collection.mutable.ArrayBuffer

/**
 * @author ilya
 * Implementation of the 2/3-consensus protocol via Akka actors
 */

/**
 * Synchrony via rounds
 */
sealed trait Consensus23Message {
  val round: Int
}

// Different kinds of messages (all parametrized by rounds)
case class DoSend(round: Int, arefs: Seq[ActorRef]) extends Consensus23Message
case class DoAsk(round: Int, id: ActorRef) extends Consensus23Message
case class DoTell[A](round: Int, value: Option[A], id: ActorRef) extends Consensus23Message

/**
 * Storing local results for a round
 */
case class LocalRoundResults[A](round: Int, num: Int, collected: ArrayBuffer[(ActorRef, A)])


class Consensus23Node[A](val myValue: A, val startingRound : Int) extends Actor {

  private var expectedRound = startingRound
  private var storedValue: Option[A] = None

  override def receive = init

  def init: Receive = {
    // A new round has started: broadcast my own value and change the role to collect the results
    case DoSend(round, arefs: Seq[ActorRef]) if round >= expectedRound =>
      // Broadcast  messages
      for (aref <- arefs) aref ! OfferValue(round, myValue, context.self)
      // transition to the collecting phase
      val lrr: LocalRoundResults[A] = LocalRoundResults(round, arefs.size, ArrayBuffer.empty)
      context.become(collectResults(lrr), discardOld = true)

    // ask for the result of the previous round  
    case DoAsk(round, id) if round + 1 == expectedRound =>
      // respond with the last taken result
      id ! DoTell(round, storedValue, context.self)
  }

  /**
   * Offer a value
   */
  private case class OfferValue(round: Int, value: A, id: ActorRef)


  def collectResults(res: LocalRoundResults[A]): Receive = {

    // Only process messages from this round
    case OfferValue(round, v, id) if round == res.round =>
      // record the value
      res.collected += ((id, v))
      // if collected all results update and switch to the initial phase
      if (res.num == res.collected.size) {

        decideAndUpdate(res.collected.map(_._2).toSeq)

        // increment round count
        expectedRound = res.round + 1

        // get back to the initial state
        context.become(init)
      }
  }

  // Decide on the value according to the collected results
  private def decideAndUpdate(results: Seq[A]) = {
    val num = results.size
    if (num == 0) {
      storedValue = None
    } else {
      val freqs = results.map(e => (e, results.count(_ == e)))
      def mostFrequent(f: Int) = freqs.forall { case (_, f1) => f1 <= f }

      // Find the most frequent element
      freqs.find { case (_, f) => mostFrequent(f) } match {
        case None => storedValue = None
        case Some((e, f)) => if (3 * f >= 2 * num) {
          storedValue = Some(e)
        } else {
          storedValue = None
        }
      }
    }
  }

}