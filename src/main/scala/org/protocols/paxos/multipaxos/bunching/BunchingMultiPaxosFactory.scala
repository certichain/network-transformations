package org.protocols.paxos.multipaxos.bunching

import org.protocols.paxos.combinators.BunchingSlotCombinator
import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxosFactory

/**
  * @author Ilya Sergey
  */

class BunchingMultiPaxosFactory[T] extends DisjointMultiPaxosFactory[T] with BunchingSlotCombinator[T] {
  override val AcceptorClass: Class[_] = classOf[AcceptorBunchingActor]
  override val ProposerClass: Class[_] = classOf[ProposerBunchingActor]
}
