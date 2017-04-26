package org.protocols.paxos.singledecree

import org.protocols.paxos.singledecree.monolithic.MonolithicSDPaxosFactory

/**
  * @author Ilya Sergey
  */
class MonolithicSDPaxosTests extends AbstractSingleDecreePaxosTests {

  s"All learners in Monolithic SD Paxos" must {
    s"agree on the same non-taken value" in {
      val values = List("A", "B", "C", "D", "E")
      setupAndTestInstances(values, new MonolithicSDPaxosFactory[String], 5)
    }
  }
  
}
