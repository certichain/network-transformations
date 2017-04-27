package org.protocols.paxos.multipaxos.bunching

import org.protocols.paxos.combinators.WideningSlotCombinator
import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxosFactory

/**
  * A MultiPaxos  factorythat bundles acceptors together for a specific ballot.
  * Proposes and Learners are not changed.
  *
  * @author Ilya Sergey
  */


class AcceptorWideningMultiPaxosFactory[T] extends DisjointMultiPaxosFactory[T] with WideningSlotCombinator[T] {
  override val AcceptorClass: Class[_] = classOf[AcceptorCombiningActor]
}


