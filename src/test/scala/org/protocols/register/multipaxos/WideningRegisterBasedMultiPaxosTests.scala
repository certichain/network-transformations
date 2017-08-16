package org.protocols.register.multipaxos

import org.protocols.register.RoundRegisterProvider

/**
  * @author Ilya Sergey
  */

class WideningRegisterBasedMultiPaxosTests extends GenericRegisterBasedMultiPaxosTests {

  def getRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new WideningSlotRegisterProvider[String](_system, numAcceptors)
  }

}
