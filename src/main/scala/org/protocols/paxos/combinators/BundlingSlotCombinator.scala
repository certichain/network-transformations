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
    */
  class AcceptorCombiningActor extends DisjointSlotActor {
    private var myHighestSeenBallot: Int = -1

    /**
      * A map from slots to the corresponding role protocols
      */
    private val slotAcceptorMap: mutable.Map[Slot, AcceptorRole] = mutable.Map.empty

    protected def getAcceptorForSlot(slot: Slot): PaxosRole = {
      val role = slotAcceptorMap.get(slot) match {
        case Some(r) => r.bumpUpBallot(myHighestSeenBallot); r
        case None => createNewRoleInstance(slot)
      }
      // Update the role for the slot
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
        // Get the appropriate role instance, maybe updating it as we go.
        // so the acceptor will only consider ballots larger than myHighestSeenBallot
        val roleInstance = getAcceptorForSlot(slot)
        // Perform a step
        val toSend = roleInstance.step(msg)
        // Update my largest seen ballot with respect to this ballot,
        // The following will be noop if b <= myHighestSeenBallot
        myHighestSeenBallot = Math.max(myHighestSeenBallot, b)
        // Send back the results
        toSend.foreach { case (a, m) => a ! MessageForSlot(slot, m) }
      case m => super.receive(m)
    }

    override def createNewRoleInstance(s: Slot): AcceptorRole =
      new AcceptorRole(myHighestSeenBallot) {
        val self = AcceptorCombiningActor.this.self
      }
  }

}
