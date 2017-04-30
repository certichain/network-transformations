package org.protocols.paxos.multipaxos

import akka.actor.ActorRef
import akka.testkit.TestKitBase
import org.protocols.paxos.{PaxosConfiguration, PaxosFactory}
import org.protocols.paxos.combinators.{Data, MessageForSlot, Stop, Voided}
import org.protocols.paxos.multipaxos.stoppable.StoppablePaxosFactory

/**
  * @author Ilya Sergey
  */

abstract sealed class StoppableMultiPaxosTests extends GenericMultiPaxosTests {

  // wrapping everything into data
  val stoppableTestMap1 = testMap1.mapValues(_.map(Data(_)))


  val testMap2 = Map(
    1 -> List("Stop1", "Stop2", "Stop3"),
    2 -> List("Imperial", "UCL", "Cambridge"))


  val testMap3 = Map(
    1 -> List("A", "Stop1", "C", "Stop2", "E"),
    2 -> List("Stop3", "Moscow", "Madrid", "Stop4", "Kyiv"),
    3 -> List("Cat", "Stop5", "Hamster", "Fish", "Turtle"),
    4 -> List("Bread", "Milk", "Kefir", "Stop6", "Beer"),
    5 -> List("Stop7", "UCL", "Stop8", "Cambridge", "Oxford"))

  //  val testMap4 = Map(
  //    1 -> List("Stop1", "Stop2", "Stop3", "Stop4", "Stop5"),
  //    1 -> List("A", "Stop", "C", "Stop", "E"),
  //    2 -> List("London", "Moscow", "Madrid", "Paris", "Kyiv"),
  //    3 -> List("Cat", "Stop", "Hamster", "Fish", "Turtle"),
  //    4 -> List("Bread", "Milk", "Kefir", "Sausage", "Beer"),
  //    2 -> List("Kings", "UCL", "Imperial", "Cambridge", "Oxford"))


  val stoppableTestMap2 = testMap2.mapValues(_.map(s => if (s.startsWith("Stop")) Stop(s) else Data(s)))
  val stoppableTestMap3 = testMap3.mapValues(_.map(s => if (s.startsWith("Stop")) Stop(s) else Data(s)))


  override protected def learnAcceptedValues[A](slotValueMap: Map[Int, List[A]],
                                                instance: PaxosConfiguration,
                                                factory: PaxosFactory[A]) = {
    import factory._

    val rss = super.learnAcceptedValues(slotValueMap, instance, factory)

    val rs: Seq[MessageForSlot[Any]] = rss.map(_.head).sortBy(_.slot)

    rs.foreach {
      case MessageForSlot(i, LearnedAgreedValue(m, _)) =>
        m match {
          case Stop(_) => rs.foreach {
            case MessageForSlot(j, LearnedAgreedValue(n, _)) =>
              if (j > i) {
                assert(n.isInstanceOf[Voided] || n.isInstanceOf[Stop],
                s"Message $n in slot $j should be Voided or Stop, since slot $i has message $m")
              }
            case _ => assert(false, "Strange message")
          }
          case _ =>
        }

      case _ => assert(false, "Strange message")
    }
    rss
  }

}


class StoppablePaxosAsPaxos extends StoppableMultiPaxosTests {
  s"All learners in a Stoppable Paxos" must {
    s"agree on the same non-taken value, if there are not stop commands" in {
      // A map from slots to values
      setupAndTestInstances(stoppableTestMap1, new StoppablePaxosFactory[String])
    }
  }
}


class StoppablePaxosStops2 extends StoppableMultiPaxosTests {
  s"A Stoppable Paxos" must {
    s"cease to proceed for further slots once a stoppable command is issued" in {
      // A map from slots to values
      setupAndTestInstances(stoppableTestMap2, new StoppablePaxosFactory[String])
    }
  }
}

class StoppablePaxosStops3 extends StoppableMultiPaxosTests {
  s"A Stoppable Paxos" must {
    s"cease to proceed for further slots once a stoppable command is issued" in {
      // A map from slots to values
      setupAndTestInstances(stoppableTestMap3, new StoppablePaxosFactory[String])
    }
  }
}

class StoppablePaxosStops4 extends StoppableMultiPaxosTests {
  s"A Stoppable Paxos" must {
    s"cease to proceed for further slots once a stoppable command is issued" in {
      // A map from slots to values
      setupAndTestInstances(stoppableTestMap3, new StoppablePaxosFactory[String])
    }
  }
}


