package org.protocols.paxos.register

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.AskableActorRef
import org.protocols.paxos.PaxosRoles
import org.protocols.paxos.singledecree.SingleDecreePaxosFactory

/**
  * @author Ilya Sergey
  */
class SingleDecreeRegisterFactory[T](val system: ActorSystem, val numA: Int)
    extends SingleDecreePaxosFactory[T] with PaxosRoles[T] {

  // cannot be larger than the number of proposers
  private var numProposals = 0
  val paxos = createPaxosInstance(system, numA, 0, 0)

  // Returns a single-served register to propose
  def getRegisterToPropose: RoundBasedRegister[T] = {
    val k = numProposals
    numProposals = numProposals + 1
    val regActor: AskableActorRef = system.actorOf(Props(classOf[SingleDecreeRegisterActor], this, paxos.acceptors, k), name = s"RegisterMiddleman-P$k")
    new RoundBasedRegister[T](numA, regActor, k)
  }

  // This should serve as middleware
  /*
   * TODO: This should "virtualise" the proposer side
   */
  class SingleDecreeRegisterActor(acceptors: Seq[ActorRef], k: Ballot) extends Actor {

    private var cid: Option[ActorRef] = None

    def receive: Receive = {
      // Messages from the client (i.e., register)
      case READ(j, `k`) =>
        if (cid.isEmpty) cid = Some(sender())
        acceptors(j) ! Phase1A(k, self)

      case WRITE(j, `k`, vW) =>
        assert(cid.nonEmpty, "By the moment of writing the sender should be determined!")
        acceptors(j) ! Phase2A(k, self, vW.asInstanceOf[T], k)

      case Phase1B(true, a, vOpt) =>
        val (kW, v) = vOpt match {
          case Some((b, value)) => (b, value)
          case None => null
        }
        ackREAD(k, kW, v)

      case Phase2B(b, a, _) =>
        assert(b == k, s"Accepted ($b) and proposed ($k) ballots should be the same")
        ackWRITE(b)
    }
  }

  // TODO: add the acceptor wrapper here and package everything as a single-decree paxos-based register
}
