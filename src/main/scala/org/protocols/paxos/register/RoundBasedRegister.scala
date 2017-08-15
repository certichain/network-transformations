package org.protocols.paxos.register

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorRef
import akka.util.Timeout

import scala.collection.immutable.Nil
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Register-based messages
  */
abstract sealed class RegisterMessage {
  // An actor to send this message to
  def dest: ActorRef
}

final case class READ(cid: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage
final case class ackREAD(j: ActorRef, dest: ActorRef, k: Int, kWv: Option[(Int, Any)]) extends RegisterMessage
final case class nackREAD(j: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage

final case class WRITE(cid: ActorRef, dest: ActorRef, k: Int, vW: Any) extends RegisterMessage
final case class ackWRITE(j: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage
final case class nackWRITE(j: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage

/**
  * @param acceptors identifiers of acceptors to communicate with through the proxy
  * @param myProxy   middleman for virtualisation, which talks to acceptors on my belaf
  * @param k         my proposer's ballot
  *
  *
  *                  Here, the "point-cut" is "acceptors ? ..." which knows how to redirect the message.
  *                  In the future, cid will be responsible for attaching extra information, like, e.g., a slot.
  *
  *                  So getRegister(s) implemented somewhere else will make sure that cid dispatches the request
  *                  to the correct slot.
  */
class RoundBasedRegister[T](private val acceptors: Seq[ActorRef],
                            // In fact, this is going to be a local proxy
                            private val myProxy: ActorRef,
                            private val msgQueue: ConcurrentLinkedQueue[Any],
                            val k: Int) {

  private val n = acceptors.size
  implicit val timeout = Timeout(5 seconds)
  val self: ActorRef = myProxy

  // resorting to shared memory concruuency
  private def processMsgs(f: Any => Unit): Unit = {
    val iter = msgQueue.iterator()
    while (iter.hasNext) {
      val msg = iter.next()
      iter.remove()
      f(msg)
    }
  }


  def read(): (Boolean, Option[T]) = {
    for (j <- acceptors) yield emitMsg(READ(self, j, k))
    var maxKW = 0
    var maxV: Option[T] = None
    var responses = 0

    Thread.sleep(1000)
    //    println

    processMsgs {
      case m@ackREAD(j, _, `k`, kWv) =>
        //        println(s"[READ] Received: $m")
        responses = responses + 1
        kWv match {
          case Some((kW, v)) if kW >= maxKW =>
            maxKW = kW
            maxV = Some(v.asInstanceOf[T])
          case _ =>
        }

      case nackREAD(j, _, `k`) => return (false, None)
      case _ => // Do nothing after the time-out
    }

    if (responses >= Math.ceil((n + 1) / 2)) (true, maxV) else (false, None)
  }

  private def write(vW: T): Boolean = {
    for (j <- acceptors) yield emitMsg(WRITE(self, j, k, vW))

    Thread.sleep(1000)
    //    println

    var responses = 0
    processMsgs {
      case m@ackWRITE(j, _, `k`) =>
        //        println(s"[WRITE] Received: $m")
        responses = responses + 1
        if (responses >= Math.ceil((n + 1) / 2)) return true
      case nackWRITE(j, _, `k`) => return false
      case _ => // Do nothing after the time-out
    }

    false
  }

  def propose(v0: T): Option[T] = {
    val readResult = read()
    readResult match {
      case (true, vOpt) =>
        val vW = if (vOpt.isEmpty) v0 else vOpt.get
        val res = write(vW)
        if (res) Some(vW) else None
      case (false, _) => None
    }
  }

  private def emitMsg(msg: Any): Unit = {
    // import akka.pattern.ask
    // ask(myProxy, msg)
    myProxy ! msg
  }

}

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
      if (read >= k) {
        emitMsg(nackREAD(self, cid, k))
      } else {
        bumpUpBallot(k)
        emitMsg(ackREAD(self, cid, k, findMaxBallotAccepted(chosenValues)))
      }

    case WRITE(cid, `self`, k, vW) =>
      if (read > k) {
        emitMsg(nackWRITE(self, cid, k))
      } else {
        // record the value
        chosenValues = (k, vW) :: chosenValues
        read = k
        // we may even ignore this step
        emitMsg(ackWRITE(self, cid, k))
      }
  }

  // This method is _always_ safe to run, as it only reduces the set of Acceptor's behaviors
  def bumpUpBallot(b: Ballot): Unit = {
    if (b > read) {
      read = b
    }
  }

  // Some library functions
  def findMaxBallotAccepted(chosenValues: List[(Ballot, Any)]): Option[(Ballot, Any)] =
    chosenValues match {
      case Nil => None
      case x => Some(x.maxBy(_._1))
    }

  // Adapt the message for the wrapping combinator
  private def emitMsg(msg: RegisterMessage) = msg

}






