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
      new AcceptorRole() {
        val self = AcceptorBunchingActor.this.self
      }
  }


  /**
    * A "smart" actor, bunching together Phase2B ("agree") messages across multiple slots.
    *
    * [REMARK] This combining actor is only valid in if acceptors are managed by `AcceptorBunchingActor`, i.e.,
    * we can ensure that quorum for one slot would imply a quorum across multiple slots, and the incoming messages
    * in this case are Bunched2A
    *
    * [REMARK] The reason why we actually need to bunch responses is explained at the end the file WideningSlotCombinator
    *
    */
  class ProposerBunchingActor(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends DisjointSlotActor {

    protected val myConvincedAcceptors: mutable.Set[ActorRef] = mutable.Set.empty

    override def receive: Receive = {
      case BunchedPhase1B(acc, slotVals) =>
        // [REMARK] Since we've received this message, the acceptor has agreed for all the slots,
        // so we record him as a convinced acceptor for all the slots
        myConvincedAcceptors.add(acc)

        // This is the most important part: it first executes all of the machines' steps,
        // so the effects (like computing the right val2a) will be in effect
        val toSendAll: Seq[(Slot, ToSend)] = for ((s, vOpt) <- slotVals) yield {
          val roleInstance = getMachineForSlot(s)
          (s, roleInstance.step(Phase1B(true, acc, vOpt)))
        }

        // Now, the post-processing is done with all proposer-machines in the updated state
        toSendAll.foreach { case (s, toSend) =>
          val postProcessed = postProcess(s, toSend)
          postProcessed.foreach { case (a, m) => a ! MessageForSlot(s, m) }
        }

      // If there was a non-trivial value accepted, it means the corresponding proposer at the corresponding
      // will be updated for it. Yet, we still need to bring freshly allocated
      // proposer up to date wrt. convinced acceptors, hence the update in `createNewRoleInstance`


      case m => super.receive(m)
    }


    override def createNewRoleInstance(s: Slot): ProposerRole = {
      new ProposerRole(acceptors, myBallot) {
        val self = ProposerBunchingActor.this.self
      }
    }

  }

}
