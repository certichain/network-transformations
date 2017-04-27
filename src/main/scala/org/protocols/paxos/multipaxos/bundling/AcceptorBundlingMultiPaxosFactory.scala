package org.protocols.paxos.multipaxos.bundling

import org.protocols.paxos.combinators.BundlingSlotCombinator
import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxosFactory

/**
  * A MultiPaxos  factorythat bundles acceptors together for a specific ballot.
  * Proposes and Learners are not changed.
  *
  * @author Ilya Sergey
  */


class AcceptorBundlingMultiPaxosFactory[T] extends DisjointMultiPaxosFactory[T] with BundlingSlotCombinator[T] {
  override val AcceptorClass: Class[_] = classOf[AcceptorCombiningActor]
}


