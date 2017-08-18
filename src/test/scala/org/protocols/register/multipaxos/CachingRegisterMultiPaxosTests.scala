package org.protocols.register.multipaxos

import org.protocols.register.RoundRegisterProvider

/**
  * @author Ilya Sergey
  */

class CachingRegisterMultiPaxosTests extends GenericRegisterMultiPaxosTests {

  def makeRegisterProvider(numAcceptors: Int): RoundRegisterProvider[String] = {
    new CachingBunchingRegisterProvider[String](_system, numAcceptors)
  }

}
