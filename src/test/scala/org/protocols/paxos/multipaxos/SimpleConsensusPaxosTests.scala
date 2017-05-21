package org.protocols.paxos.multipaxos

import org.protocols.paxos.combinators.Command
import org.protocols.paxos.multipaxos.mencius.SimpleConsensusMultiCombinatorFactory

/**
  * @author Ilya Sergey
  */

class SimpleConsensusPaxosTests extends GenericMultiPaxosTests {

  // wrapping everything into data
  val restrictedTestData = testMap1.mapValues(_.map(Command(_)))


  s"All learners in a fully Acceptor-bundling Multi Paxos" must {
    s"agree on the same non-taken value" in {
      // A map from slots to values
      //      setupAndTestInstances(testMap1, new BunchingMultiPaxosFactory[String])
      //      setupAndTestInstances(restrictedTestData, new BunchingMultiPaxosFactory[CommandOrNoOp[String]])
      setupAndTestInstances(restrictedTestData, new SimpleConsensusMultiCombinatorFactory[String])
    }
  }

}