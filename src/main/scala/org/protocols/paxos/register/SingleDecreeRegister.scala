package org.protocols.paxos.register

import akka.pattern.AskableActorRef
import akka.util.Timeout
import org.protocols.paxos.PaxosRoles

import scala.concurrent.Await
import scala.concurrent.duration._
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
  * @param cid          middleman for virtualisation, which talks to acceptors on my belaf
  * @param k            ballot
  */
class SingleDecreeRegister[T](private val numAcceptors: Int,
                              // In fact, this is going to be a local proxy
                              private val cid: AskableActorRef,
                              val k: Int) extends PaxosRoles[T] {

  implicit val timeout = Timeout(5 seconds)

  private def read(): Option[T] = {
    val fs = for (j <- 0 until numAcceptors) yield cid ? READ(j, k)
    var maxKW = 0
    var maxV: Option[T] = None
    var responses = 0

    // Process responses
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
    val fs = for (j <- 0 until numAcceptors) yield cid ? WRITE(j, k, vW)
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




