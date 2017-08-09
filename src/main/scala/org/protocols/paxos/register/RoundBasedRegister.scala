package org.protocols.paxos.register

import akka.pattern.AskableActorRef
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

// Messages
final case class READ(j: Int, k: Int)
final case class ackREAD(k: Int, kW: Int, v: Any)
final case class nackREAD(k: Int)
final case class WRITE(j: Int, k: Int, vW: Any)
final case class ackWRITE(k: Int)
final case class nackWRITE(k: Int)

/**
  * @param numAcceptors number of acceptors acceptors
  * @param acceptors    middleman for virtualisation, which talks to acceptors on my belaf
  * @param k            ballot
  *
  *
  *                     Here, the "point-cut" is "acceptors ? ..." which knows how to redirect the message.
  *                     In the future, cid will be responsible for attaching extra information, like, e.g., a slot.
  *
  *                     So getRegister(s) implemented somewhere else will make sure that cid dispatches the request
  *                     to the correct slot.
  */
class RoundBasedRegister[T](private val numAcceptors: Int,
                            // In fact, this is going to be a local proxy
                            private val acceptors: AskableActorRef, val k: Int) {

  implicit val timeout = Timeout(5 seconds)

  // TODO: This "?" should provide an implementation by means of an actor
  def emitMsg(msg: Any): Future[Any] = acceptors ? msg

  private def read(): Option[T] = {
    val fs = for (j <- 0 until numAcceptors) yield emitMsg(READ(j, k))
    var maxKW = 0
    var maxV: Option[T] = None
    var responses = 0

    // Process responses from "acceptors"
    for (f <- fs) Await.result(f, timeout.duration) match {
      case ackREAD(`k`, kW, v) =>
        responses = responses + 1
        if (kW >= maxKW) {
          maxKW = kW
          maxV = Some(v.asInstanceOf[T])
        }
      case nackREAD(`k`) => return None
      case _ => // Do nothing after the time-out
    }
    if (responses >= Math.ceil((numAcceptors + 1) / 2)) maxV else None
  }

  private def write(vW: T): Boolean = {
    val fs = for (j <- 0 until numAcceptors) yield emitMsg(WRITE(j, k, vW))
    var responses = 0
    for (f <- fs) Await.result(f, 5 seconds) match {
      case ackWRITE(`k`) =>
        responses = responses + 1
        if (responses >= Math.ceil((numAcceptors + 1) / 2)) return true
      case nackWRITE(`k`) => return false
      case _ => // Do nothing after the time-out
    }
    false
  }

  def propose(v0: T): Option[T] = {
    val readResult = read()
    readResult match {
      case Some(v) =>
        val vW = if (v == null) v0 else v
        val res = write(vW)
        if (res) Some(v) else None
      case None => None
    }
  }
}


// TODO: Write an acceptor next to it, so the composition would be straightforward via connecting one to another





