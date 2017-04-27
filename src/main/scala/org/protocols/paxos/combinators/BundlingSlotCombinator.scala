package org.protocols.paxos.combinators

import org.protocols.paxos.PaxosRoles

import scala.collection.mutable

/**
  * @author Ilya Sergey
  */

trait BundlingSlotCombinator[T] extends SlotReplicatingCombinator[T] with PaxosRoles[T] {

  /**
    * This is a tailored actor that gives special treatment to Phase1a messages for acceptors:
    * - It bumps up all acceptors' current ballot according to what has been last;
    * - It keeps track of the highest seen ballot as an internal variable.
    *
    * The (lazy) invariant of this actor is that all acceptor instances, controlled by it, are operating as their
    * highest seen ballots are the same as myHighestSeenBallot (which is ensured via getMachineForSlot).
    *
    */
  class AcceptorCombiningActor extends DisjointSlotActor {
    private var myHighestSeenBallot: Int = -1

    /**
      * A map from slots to the corresponding role protocols
      */
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

    /**
      * This receive performs "widening" with respect to the Phase1A logic of an acceptor.
      * Specifically, instead of updating the ballot for just one acceptor in its slot-map,
      * it will "bump up" the ballots for _all_ of them (lazily), via myHighestSeenBallot
      */
    override def receive: Receive = {
      case MessageForSlot(slot, msg@Phase1A(b, l)) =>
        // Update my largest seen ballot with respect to this ballot,
        // The following will be no-op if b <= myHighestSeenBallot
        myHighestSeenBallot = Math.max(myHighestSeenBallot, b)

        // Get the appropriate role instance, maybe updating it for the last ballot as we go.
        // so the acceptor will only consider ballots larger than myHighestSeenBallot

        /**
          * [REMARK]
          * Here, it would be perfectly safe to send back only the result for `slot`, but since we have bumped
          * up all our acceptor instances, we can as well inform this proposer about all values we have accepted for
          * any slots ever.
          *
          * TODO: prove that this is a safe transformation from sending the response just fortheis specific `slot`.
          * As tests show, with the unmodified proposer, this has no effect, as other messages are being ignored anyway
          * (since we haven't asked for them).
          *
          * [REMARK] This is a good candidate for "redundancy" transformation, enabled by monotonicity of the system.
          */
        for (s <- slotAcceptorMap.keySet + slot) {
          val roleInstance = getMachineForSlot(s)
          // Send back the results for all slots
          roleInstance.step(msg).foreach { case (a, m) => a ! MessageForSlot(s, m) }
        }

      case m =>
        super.receive(m)
    }

    override def createNewRoleInstance(s: Slot): AcceptorRole =
      new AcceptorRole() {
        val self = AcceptorCombiningActor.this.self
      }
  }

}
