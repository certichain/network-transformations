package org.protocols.paxos.combinators

import akka.actor.ActorRef
import org.protocols.paxos.PaxosRoles

import scala.collection.Map

/**
  * A combinator for stoppable functionality
  *
  * @author Ilya Sergey
  */

trait StoppableSlotCombinator[T] extends BunchingSlotCombinator[DataOrStop[T]] with PaxosRoles[DataOrStop[T]] {

  /**
    * For stoppable functionality, we only need to change the proposer logic, not the acceptors
    */
  class StoppableProposerActor(override val acceptors: Seq[ActorRef], override val myBallot: Ballot)
      extends ProposerBunchingActor(acceptors, myBallot) {

    import collection.mutable.{Map => MMap}

    /**
      * A map storing all proposed values along with the ballots for
      * the value that has been replicated (i.e., mbal2a from the Stoppable- Paos paper)
      */
    /**
      * Analyse the output of the proposer in order to decide whether to forward it or not;
      *
      * [REMARK] characterise this transformation as monotonic
      */
    override def postProcess(i: Slot, toSend: ToSend): ToSend = toSend match {
      // Only trigger if we're dealing with the Phase2A message
      case p2as@((_, Phase2A(_, _, _, mbal_i)) :: _) =>

        // Simple sanity check
        assert(p2as.forall(_._2.isInstanceOf[Phase2A]), s"All messages should be Phase2A:\n$p2as")
        val (_, Phase2A(_, _, data, _)) = p2as.head // all other are identical

        // Get slot/proposal information
        val slotToProposedVal: Map[Slot, (Option[DataOrStop[T]], Ballot)] =
          slotMachineMap.mapValues { p =>
            val (d, bal, _) = p.asInstanceOf[ProposerRole].val2a(None)
            (d, bal)
          }

        // Now the most interesting stage: decide whether we can send `stop`
        data match {
          // Only forward the messages if there is no preceding stop command
          case Data(d) =>
            val earlierStop = slotToProposedVal.exists {
              // All slots j < i are not stop commands
              case (j, (vOpt, mbal_j)) => j < i && vOpt.nonEmpty && vOpt.get.isStop
            }
            if (earlierStop) createVoidMessages(p2as, "Data (Earlier Stop)") else p2as

          // Decide whether we can emit stop given our accumulated record for slots
          case Stop(s) =>
            val shouldVoidStop = slotToProposedVal.exists {
              // A condition from Stoppable Paxos
              case (j, (vOpt, mbal_j)) => j > i && mbal_j > mbal_i
            }
            // Void the command if there are later non-stops
            if (shouldVoidStop) {
              println(s"Voiding stop $s...")
              createVoidMessages(p2as, "Stop (Later Data)")
            }
            else p2as
          case _ => Nil
        }
      case xs => xs
    }
  }

  def createVoidMessages(ps: Seq[(ActorRef, PaxosMessage)], reason: String) =
    ps.asInstanceOf[Seq[(ActorRef, Phase2A)]].map {
      case (a, Phase2A(mb, p, _, mbal)) => (a, Phase2A(mb, p, Voided(reason), mbal))
    }

}

/**
  * An option-kind class signifying data or stop command
  */
abstract sealed class DataOrStop[+M] {
  def isStop: Boolean
}
case class Data[M](data: M) extends DataOrStop[M] {
  override def isStop: Boolean = false
}
case class Stop(id: String) extends DataOrStop[Nothing] {
  override def isStop: Boolean = true
}

case class Voided(reason: String) extends DataOrStop[Nothing] {
  override def isStop: Boolean = false
}

