package org.protocols.paxos.multipaxos.stoppable

import org.protocols.paxos.combinators.{DataOrStop, StoppableSlotCombinator}
import org.protocols.paxos.multipaxos.bunching.BunchingMultiPaxosFactory

/**
  * @author Ilya Sergey
  */

class StoppablePaxosFactory[T] extends BunchingMultiPaxosFactory[DataOrStop[T]]
    with StoppableSlotCombinator[T] {

  override val ProposerClass: Class[_] = classOf[StoppableProposerActor]

}

