package org.protocols.register

import akka.actor.ActorRef
import org.protocols.register.util.{AcceptorAux, RegisterAux}

import scala.collection.immutable.Nil
import scala.language.postfixOps

/**
  * An acceptor STS
  *
  * @param initRead Initial ballot to start from
  */
class AcceptorForRegister(val self: ActorRef,
                          private val initRead: Int = 0) extends AcceptorAux {

  var read: Ballot = initRead
  var chosenValues: List[(Ballot, Any)] = Nil

  val step: Step = {
    case m@READ(cid, `self`, k) =>
      // Using non-strict inequality here for multi-paxos
      if (k >= read) {
        read = k
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
                            val myProxy: ActorRef,
                            val k: Int,
                            val contextParams: Any) extends RegisterAux {

  val quorumSize = Math.ceil((acceptors.size + 1) / 2).toInt

  def read(): (Boolean, Option[T]) = {
    // Send out requests
    for (j <- acceptors) yield emitMsg(READ(self, j, k))

    // Collect responses
    var maxKW = 0
    var maxV: Option[T] = None

    // Collect both positive and negative responses
    var yesResponses: Set[ActorRef] = Set.empty
    var noResponses: Set[ActorRef] = Set.empty

    processInbox {
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
    processInbox {
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

}





