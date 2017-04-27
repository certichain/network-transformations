package org.protocols.paxos.multipaxos

import org.protocols.paxos.multipaxos.disjoint.DisjointMultiPaxosFactory

/**
  * @author Ilya Sergey
  */

class DisjointMultiPaxosTests extends GenericMultiPaxosTests {

  s"All learners in a fully slot-disjoint Multi Paxos" must {
    s"agree on the same non-taken value" in {
      // A map from slots to values
      setupAndTestInstances(testMap1, new DisjointMultiPaxosFactory[String])
    }
  }

}
