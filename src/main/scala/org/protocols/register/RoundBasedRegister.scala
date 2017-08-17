package org.protocols.register

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorRef

import scala.collection.immutable.Nil
import scala.language.postfixOps


/**
  * An acceptor STS
  *
  * @param initRead Initial ballot to start from
  */
class AcceptorForRegister(val self: ActorRef,
                          private val initRead: Int = 0) {

  type Ballot = Int
  type ToSend = RegisterMessage
  type Step = PartialFunction[Any, RegisterMessage]

  var read: Ballot = initRead
  var chosenValues: List[(Ballot, Any)] = Nil

  val step: Step = {
    case m@READ(cid, `self`, k) =>
      // Using non-strict inequality here for multi-paxos
      if (k >= read) {
        bumpUpBallot(k)
        emitMsg(ackREAD(self, cid, k, findMaxBallotAccepted(chosenValues)))
      } else {
        emitMsg(nackREAD(self, cid, k, findMaxBallotAccepted(chosenValues)))
      }

    case WRITE(cid, `self`, k, vW) =>
      if (read == k) {
        // record the value
        chosenValues = (k, vW) :: chosenValues
        read = k
        // we may even ignore this step
        emitMsg(ackWRITE(self, cid, k))
      } else {
        emitMsg(nackWRITE(self, cid, k))
      }
  }


  /** ****************************************************************************
    * Utility methods and auxiliary fields
    * ****************************************************************************/

  // This method is _always_ safe to run, as it only reduces the set of Acceptor's behaviors
  def bumpUpBallot(b: Ballot): Unit = if (b > read) {
    read = b
  }

  // Some library functions
  private def findMaxBallotAccepted(chosenValues: List[(Ballot, Any)]) = chosenValues match {
    case Nil => None
    case x => Some(x.maxBy(_._1))
  }

  // Adapt the message for the wrapping combinator
  private def emitMsg(msg: RegisterMessage) = msg

}


/**
  * A reusable round-based register implementation
  *
  * @param acceptors     identifiers of acceptors to communicate with through the proxy
  * @param myProxy       A middleman for virtualisation of message handling
  * @param k             my ballot
  * @param contextParams parameters per this instance, passed to the proxy (e.g., a slot)
  **/
class RoundBasedRegister[T](private val acceptors: Seq[ActorRef],
                            private val myProxy: ActorRef,
                            val k: Int,
                            val contextParams: Any) {

  private val quorumSize = Math.ceil((acceptors.size + 1) / 2)

  def read(): (Boolean, Option[T]) = {
    // Send out requests
    for (j <- acceptors) yield emitMsg(READ(self, j, k))

    // Collect responses
    var maxKW = 0
    var maxV: Option[T] = None

    // Collect both positive and negative responses
    var yesResponses: Set[ActorRef] = Set.empty
    var noResponses: Set[ActorRef] = Set.empty

    processIncomingMessages {
      case m@ackREAD(j, _, `k`, kWv) =>
        // Accounting for duplicate messages
        if (!yesResponses.contains(j)) {
          yesResponses = yesResponses + j
          kWv match {
            case Some((kW, v)) if kW >= maxKW =>
              maxKW = kW
              maxV = Some(v.asInstanceOf[T])
            case _ =>
          }
        }

      case nackREAD(j, _, `k`, kWv) =>
        // Accounting for duplicate messages
        if (!noResponses.contains(j)) {
          noResponses = noResponses + j
          // Learn the value anyway
          kWv match {
            case Some((kW, v)) if kW >= maxKW =>
              maxKW = kW
              maxV = Some(v.asInstanceOf[T])
            case _ =>
          }
        }
    }

    // Return result of reading
    if (yesResponses.size >= quorumSize) (true, maxV) else (false, maxV)
  }

  private def write(vW: T): Boolean = {
    // Send out proposals
    for (j <- acceptors) yield emitMsg(WRITE(self, j, k, vW))

    // Collect responses
    var yesResponses: Set[ActorRef] = Set.empty
    processIncomingMessages {
      case m@ackWRITE(j, _, `k`) =>
        if (!yesResponses.contains(j)) {
          yesResponses = yesResponses + j
          if (yesResponses.size >= quorumSize) {
            return true
          }
        }
      case nackWRITE(j, _, `k`) => return false
    }
    false
  }

  def propose(v0: T): Option[T] = {
    val readResult = read()
    readResult match {
      case (true, vOpt) =>
        val vW = if (vOpt.isEmpty) v0 else vOpt.get
        val res = write(vW)
        if (res) {
          Some(vW)
        } else None
      case (false, _) => None
    }
  }


  /** ****************************************************************************
    * Utility methods and auxiliary fields
    * ****************************************************************************/

  private val myMailbox: ConcurrentLinkedQueue[Any] = new ConcurrentLinkedQueue[Any]()
  private val timeoutMillis = 100
  private val self: ActorRef = myProxy // Middleman for virtualisation

  private def emitMsg(msg: RegisterMessage): Unit = {
    self ! MessageToProxy(msg, contextParams)
  }

  def deliver(msg: Any): Unit = {
    myMailbox.synchronized {
      // Need to synchronize in order to avoid infinite loops with `processIncomingMessages`
      myMailbox.add(msg)
    }
  }

  /**
    * Processing the messages in the mailbox.
    * Resorting to shameful shared-memory concurrency... [sigh].
    *
    * @param f function to select and process messages
    */
  private def processIncomingMessages(f: PartialFunction[Any, Unit]) {
    // Wait until enough messages received instead of timeout
    var shouldProcess = true

    // Loop while not sufficiently many mails collected
    while (shouldProcess) {
      myMailbox.synchronized {
        val iter = myMailbox.iterator()
        var inbox: Set[Any] = Set.empty
        while (iter.hasNext) {
          val msg = iter.next()
          if (f.isDefinedAt(msg) && !inbox.contains(msg)) {
            inbox = inbox + msg
          }
        }
        // Collected sufficiently many letters to process: can now shoot
        if (inbox.size >= quorumSize) {
          // Process all the collected incoming mails with f
          for (m <- inbox) {
            f(m)
          }
          // Clean the inbox from similar messages
          myMailbox.removeIf(f.isDefinedAt(_))
          shouldProcess = false
        }
      }
    }
  }

}

/////////////////////////////////////////////////////////////////////////////////
/**
  * Register-based messages
  */
/////////////////////////////////////////////////////////////////////////////////
abstract sealed class RegisterMessage {
  // Source of the message
  def src : ActorRef
  // An actor to send this message to
  def dest: ActorRef
  // Ballot, subject of interaction
  def k: Int
}

final case class READ(src: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage
final case class ackREAD(src: ActorRef, dest: ActorRef, k: Int, kWv: Option[(Int, Any)]) extends RegisterMessage
final case class nackREAD(src: ActorRef, dest: ActorRef, k: Int, kWv: Option[(Int, Any)]) extends RegisterMessage

final case class WRITE(src: ActorRef, dest: ActorRef, k: Int, vW: Any) extends RegisterMessage
final case class ackWRITE(src: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage
final case class nackWRITE(src: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage

case class MessageToProxy(rm: RegisterMessage, params: Any)





