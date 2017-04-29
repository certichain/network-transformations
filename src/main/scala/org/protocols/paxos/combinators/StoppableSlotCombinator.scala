package org.protocols.paxos.combinators

import akka.actor.ActorRef
import org.protocols.paxos.PaxosRoles

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
    case class ProposeRecord(mBal: Ballot, dataOrStop: DataOrStop[T])
    private val mySlotToProposedVal: MMap[Slot, ProposeRecord] = MMap.empty

    /**
      * Analyse the output of the proposer in order to decide whether to forward it or not;
      *
      * [REMARK] characterise this transformation as monotonic
      */
    override def postProcess(i: Slot, toSend: ToSend): ToSend = toSend match {
      // Only trigger if we're dealing with the Phase2A message
      case p2as@((_, Phase2A(_, _, _, _)) :: _) =>
        // Simple sanity check
        assert(p2as.forall(_._2.isInstanceOf[Phase2A]), s"All messages should be Phase2A:\n$p2as")
        val (_, Phase2A(_, _, data, mbal)) = p2as.head // all other are identical

        // Update record for this send
        for ((a, Phase2A(_, _, data, mbal_i)) <- p2as) {
          mySlotToProposedVal.update(i, ProposeRecord(mbal_i, data))
        }

        // Now the most interesting stage: decide whether we can send `stop`
        data match {
          // Only forward the messages if there is no preceding stop command
          case Data(d) =>
            val earlierSlotsAreNotStops = mySlotToProposedVal.forall {
              // All slots j < i are not stop commands
              case (j, ProposeRecord(_, v)) => j >= i || !v.isStop
            }

            if (earlierSlotsAreNotStops) p2as else Nil

          // Decide whether we can emit stop given our accumulated record for slots
          case Stop =>
            val existsLaterNonStopSlot = mySlotToProposedVal.exists {
              // All slots j > i are not slot commands
              case (j, ProposeRecord(mbal_j, v)) => j > i || !v.isStop
            }

            // Void the command if there are later non-stops
            if (existsLaterNonStopSlot) Nil else p2as
        }
      case xs => xs
    }
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
case object Stop extends DataOrStop[Nothing] {
  override def isStop: Boolean = true
}

