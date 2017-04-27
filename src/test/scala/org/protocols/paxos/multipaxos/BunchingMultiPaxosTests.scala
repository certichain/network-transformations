package org.protocols.paxos.multipaxos

import org.protocols.paxos.multipaxos.bunching.BunchingMultiPaxosFactory

/**
  * @author Ilya Sergey
  */

class BunchingMultiPaxosTests extends GenericMultiPaxosTests {

  s"All learners in a fully Acceptor-bundling Multi Paxos" must {
    s"agree on the same non-taken value" in {
      // A map from slots to values
      setupAndTestInstances(testMap1, new BunchingMultiPaxosFactory[String])
    }
  }

}
