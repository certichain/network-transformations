package org.protocols.paxos.multipaxos

import org.protocols.paxos.multipaxos.bundling.AcceptorBundlingMultiPaxosFactory

/**
  * @author Ilya Sergey
  */

class AcceptorBundlingMultiPaxosTests extends GenericMultiPaxosTests {

  s"All learners in a fully Acceptor-bundling Multi Paxos" must {
    s"agree on the same non-taken value" in {
      // A map from slots to values
      setupAndTestInstances(testMap1, new AcceptorBundlingMultiPaxosFactory[String])
    }
  }

}
