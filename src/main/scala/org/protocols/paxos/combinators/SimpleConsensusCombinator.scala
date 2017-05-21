package org.protocols.paxos.combinators

import akka.actor.ActorRef
import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxos

/**
  * A simple combinator building actors on top of Paxos roles, so they would
  * only allow proposing messages under certain values,
  * otherwise it proposes a NoOp message.
  *
  * We have inherit it from DisjointMultiPaxos, not from BunchingSlotCombinator.
  * See [Simple Consensus and bunching] for explanations.
  *
  * @author Ilya Sergey
  */

trait SimpleConsensusCombinator[T] extends DisjointMultiPaxos[CommandOrNoOp[T]] {


  class SimpleConsensusProposerActor(acceptors: Seq[ActorRef], myBallot: Ballot,
                                     // number of proposers is known
                                     val myNumProposers: Int)
      extends MultiPaxosProposer(acceptors, myBallot) {

    /**
      * @return true if the slot to be proposed is divisible by the ballot number
      */
    def coordinatorForSlot(slot: Slot): Boolean =
      slot % myNumProposers == myBallot

    override def receive: Receive = {
      case m@MessageForSlot(s, ProposeValue(v)) =>
        if (coordinatorForSlot(s)) {
          // println(s"GOOD :: My ballot: [$myBallot], slot: [$s], message: [$m]")
          super.receive(m)
        } else {
          // do nothing
          // println(s"IGNORE :: My ballot: [$myBallot], slot: [$s], message: [$m]")
          // See [Default behavior for NoOps]
          //super.receive(MessageForSlot(s, ProposeValue(NoOp)))
        }
      case x => super.receive(x)
    }
  }

}

abstract sealed class CommandOrNoOp[+T]
case class Command[T](t: T) extends CommandOrNoOp[T]
object NoOp extends CommandOrNoOp[Nothing]

/*
[Restricted Paxos and bunching]

It seems that the Simple Consensus/Paxos is not a strict refinement of MultiPaxos with bunching, as bunching
essentially conflates multiple Phases 1 for a single proposer, making him a coordinator over multiple slots.

In contrast, in Simple Consensus, each coordinator takes over a specific subset of slots, and they do not interfere.

Thus, an attempt to inherit Simple Consensus from BunchingSlotCombinator instead of DisjointMultiPaxos
leads to starvation (as one coordinator essentially prevents others from progressing by taking over all of the slots).

How cool is that!
 */

/*
[Default behavior for NoOps]

We have an alternative: either just mute a proposal for a mismatched proposer or make it
proceed with a default "NoOp" proposal, ignored by the acceptors.

 */
