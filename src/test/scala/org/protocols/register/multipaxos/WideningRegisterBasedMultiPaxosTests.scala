package org.protocols.register.multipaxos

import org.protocols.register.RoundRegisterProvider

/**
  * @author Ilya Sergey
  */

class WideningRegisterBasedMultiPaxosTests extends GenericRegisterBasedMultiPaxosTests {

  def makeRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new WideningSlotRegisterProvider[String](_system, numAcceptors)
  }

}
