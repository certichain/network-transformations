package org.protocols.register.multipaxos

import org.protocols.register.RoundRegisterProvider

/**
  * @author Ilya Sergey
  */

class BunchingRegisterMultiPaxosTests extends GenericRegisterMultiPaxosTests {

  def makeRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new BunchingRegisterProvider[String](_system, numAcceptors)
  }

}
