package org.protocols.register.multipaxos

import org.protocols.register.RoundRegisterProvider

/**
  * @author Ilya Sergey
  */

class WideningRegisterMultiPaxosTests extends GenericRegisterMultiPaxosTests {

  def makeRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new WideningSlotRegisterProvider[String](_system, numAcceptors)
  }

}
