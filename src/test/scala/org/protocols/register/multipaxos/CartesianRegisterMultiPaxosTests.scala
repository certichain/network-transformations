package org.protocols.register.multipaxos

import org.protocols.register.RoundRegisterProvider

/**
  * @author Ilya Sergey
  */

class CartesianRegisterMultiPaxosTests extends GenericRegisterMultiPaxosTests {

  def makeRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new SlotReplicatingRegisterProvider[String](_system, numAcceptors)
  }

}
