package org.protocols.paxos.singledecree

/**
  * @author Ilya Sergey
  */
class GenericSDPaxosTests extends AbstractSingleDecreePaxosTests {

  s"All learners in Simple Single-Decree Paxos" must {
    s"agree on the same non-taken value" in {
      val values = List("A", "B", "C", "D", "E")
      setupAndTestInstances(values, new SingleDecreePaxosFactory[String], acceptorNum = 15)
    }
  }

}
