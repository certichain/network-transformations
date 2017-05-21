package org.protocols.paxos.combinators

import akka.actor.ActorRef
import org.protocols.paxos.PaxosRoles

import scala.collection.mutable

/**
  * @author Ilya Sergey
  */

trait BunchingSlotCombinator[T] extends SlotReplicatingCombinator[T] with PaxosRoles[T] {

  // A message type for a bunched acceptor response
  case class BunchedPhase1B(a: ActorRef, slotVals: Seq[(Slot, Option[(Ballot, T)])])

  /**
    * A smart acceptor combiner, bunching the responses together for all slots
    */
  class AcceptorBunchingActor extends DisjointSlotActor {

    override type Role = AcceptorRole

    private var myHighestSeenBallot: Ballot = -1

    // A map from slots to the corresponding acceptor machines
    private val slotAcceptorMap: mutable.Map[Slot, AcceptorRole] = mutable.Map.empty

    override protected def getMachineForSlot(slot: Slot): AcceptorRole = {
      val role = slotAcceptorMap.get(slot) match {
        case Some(r) => r
        case None => createNewRoleInstance(slot)
      }
      // Update the role for the slot for the highest seen ballot
      role.bumpUpBallot(myHighestSeenBallot)
      slotAcceptorMap.update(slot, role)
      role
    }

    override def receive: Receive = {
      case MessageForSlot(slot, msg@Phase1A(b, l)) =>
        myHighestSeenBallot = Math.max(myHighestSeenBallot, b)
        /* TODO: Think of linguistic terms to express this bunching
           operation as a protocol combinator */

        // For each slot, compute an accepted last value
        val slotLastValues: Seq[(Slot, Option[(Ballot, T)])] =
          (for (s <- slotAcceptorMap.keySet + slot) yield {
            val roleInstance = getMachineForSlot(s)
            roleInstance.step(msg) match {
              case Nil => Nil
              case (`l`, Phase1B(true, `self`, vOpt)) :: Nil =>
                Seq((s, vOpt))
            }
          }).flatten.toSeq

        l ! BunchedPhase1B(self, slotLastValues)

      case m => super.receive(m)
    }

    override def createNewRoleInstance(s: Slot): AcceptorRole =
      createAcceptor(AcceptorBunchingActor.this.self)
  }

  // A "smart" actor, bunching together Phase2B ("agree") messages across multiple slots.
  class ProposerBunchingActor(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends DisjointSlotActor {

    override type Role = ProposerRole

    protected var myConvincedAcceptors: Set[(ActorRef, Option[(Ballot, T)])] = Set.empty

    override def receive: Receive = {
      case BunchedPhase1B(acc, slotVals) =>
        // See [Update for all Slots]
        myConvincedAcceptors = myConvincedAcceptors ++ Set((acc, None))

        for ((s, vOpt) <- slotVals.sortBy(_._1)) {
          val proposer = getMachineForSlot(s)
          val toSend = proposer.step(Phase1B(true, acc, vOpt))
          val postProcessed = postProcess(s, toSend)
          postProcessed.foreach { case (a, m) => a ! MessageForSlot(s, m) }
        }

      case m => super.receive(m)
    }

    override protected def getMachineForSlot(slot: Slot): ProposerRole = {
      val p = super.getMachineForSlot(slot)
      // Any invoked machine needs to be brought up to date
      p.addResponses(myConvincedAcceptors)
      p
    }

    override def createNewRoleInstance(s: Slot): ProposerRole =
      createProposer(acceptors, myBallot, self)
  }

  /* [Update for all Slots]

  Since we've received this message, the acceptor has
  agreed for _all_ the slots, so we record him as a convinced acceptor for all the slots.
   */


  /* [Validity of ProposerBunchingActor]

  This combining actor is only valid in if acceptors are managed by `AcceptorBunchingActor`, i.e.,
   we can ensure that quorum for one slot would imply a quorum across multiple slots,
   and the incoming messages in this case are Bunched2A. The reason why we actually need to bunch responses
   is explained at the end the file WideningSlotCombinator, remark [Proposers and Widening].

     */
}
