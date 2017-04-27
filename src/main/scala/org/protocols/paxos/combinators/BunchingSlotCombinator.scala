package org.protocols.paxos.combinators

import akka.actor.ActorRef
import org.protocols.paxos.PaxosRoles

import scala.collection.{Set, mutable}

/**
  * @author Ilya Sergey
  */

trait BunchingSlotCombinator[T] extends SlotReplicatingCombinator[T] with PaxosRoles[T] {

  // A message type for a bunched acceptor response
  case class Bunched2A(a: ActorRef, slotVals: Seq[(Slot, Option[(Ballot, T)])])


  /**
    * A smart acceptor combiner, bunching the responses together for all slots
    */
  class AcceptorBunchingActor extends DisjointSlotActor {
    private var myHighestSeenBallot: Int = -1

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

        // For each proposer, compute an accepted value for each slot on this actor
        val proposerSlotLastValues: Seq[(ActorRef, (Slot, Option[(Ballot, T)]))] =
          (for (s <- slotAcceptorMap.keySet + slot) yield {
            val roleInstance = getMachineForSlot(s)
            val stepResult = roleInstance.step(msg).asInstanceOf[Seq[(ActorRef, Phase1B)]]
            stepResult.map { case (_, Phase1B(true, _, vOpt)) => (l, (s, vOpt)) }
          }).flatten.toSeq

        val proposerResponses: Map[ActorRef, Bunched2A] =
          proposerSlotLastValues.groupBy(_._1).mapValues(v => Bunched2A(self, v.unzip._2))

        // Send bunched responses to proposers
        proposerResponses.foreach { case (k, v) => k ! v }

      case m => super.receive(m)
    }

    override def createNewRoleInstance(s: Slot): AcceptorRole =
      new AcceptorRole() {
        val self = AcceptorBunchingActor.this.self
      }
  }


  /**
    * A "smart" agent, bunching together Phase2B ("agree") messages across multiple slots.
    *
    * [REMARK] This combining actor is only valid in if acceptors are managed by `AcceptorBunchingActor`, i.e.,
    * we can ensure that quorum for one slot would imply a quorum across multiple slots, and the incoming messages
    * in this case are Bunched2A
    *
    */
  class ProposerBunchingActor(val acceptors: Seq[ActorRef], val myBallot: Ballot) extends DisjointSlotActor {

    // A map from slots to the corresponding proposer machines
    private val slotProposerMap: mutable.Map[Slot, ProposerRole] = mutable.Map.empty

    override def receive: Receive = {
      case Bunched2A(acc, slotVals) =>
        // Process all slot-values and get feedback from sub-protocols
        for ((s, vOpt) <- slotVals) {
          val propRole = getMachineForSlot(s)
          propRole.step(Phase1B(promise = true, acc, vOpt)).foreach { case (a, m) => MessageForSlot(s, m) }
        }
      // TODO: add a separate treatment for processing an initial proposal for an already reached quorum
      // Should look for a quorum in some of the slots and then proceed
      //  Hmm... do we really need it?
      case m => super.receive(m)
    }


    override def createNewRoleInstance(s: Slot): ProposerRole =
      new ProposerRole(acceptors, myBallot) {
        val self = ProposerBunchingActor.this.self
      }

  }


}
