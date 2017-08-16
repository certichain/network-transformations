package org.protocols.paxos.register

import akka.actor.ActorSystem

/**
  * @author Ilya Sergey
  */

class CartesianRegisterBasedMultiPaxosTests extends GenericRegisterBasedMultiPaxosTests {

  def getRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new SlotReplicatingRegisterProvider[String](_system, numAcceptors)
  }

}
