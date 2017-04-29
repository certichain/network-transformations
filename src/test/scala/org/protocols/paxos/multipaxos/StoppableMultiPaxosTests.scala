package org.protocols.paxos.multipaxos

import akka.actor.ActorRef
import org.protocols.paxos.PaxosFactory
import org.protocols.paxos.combinators.{Data, MessageForSlot, Stop}
import org.protocols.paxos.multipaxos.stoppable.StoppablePaxosFactory

/**
  * @author Ilya Sergey
  */

abstract sealed class StoppableMultiPaxosTests extends GenericMultiPaxosTests {

  // wrapping everything into data
  val stoppableTestMap1 = testMap1.mapValues(_.map(Data(_)))


//  val testMap2 = Map(
//    1 -> List("A", "Stop"),
////    1 -> List("A", "Stop", "C", "Stop", "E"),
////    2 -> List("London", "Moscow", "Madrid", "Paris", "Kyiv"),
////    3 -> List("Cat", "Stop", "Hamster", "Fish", "Turtle"),
////    4 -> List("Bread", "Milk", "Kefir", "Sausage", "Beer"),
//    2 -> List("Kings", "UCL"))


  val testMap2 = Map(
    1 -> List("Stop", "Stop", "Stop", "Stop", "Stop"),
//    1 -> List("A", "Stop", "C", "Stop", "E"),
//    2 -> List("London", "Moscow", "Madrid", "Paris", "Kyiv"),
//    3 -> List("Cat", "Stop", "Hamster", "Fish", "Turtle"),
//    4 -> List("Bread", "Milk", "Kefir", "Sausage", "Beer"),
    2 -> List("Kings", "UCL", "Imperial", "Cambridge", "Oxford"))


  val testMap3 = Map(
    1 -> List("Stop", "Stop", "Stop", "Stop", "Stop"),
//    1 -> List("A", "Stop", "C", "Stop", "E"),
    2 -> List("Stop", "Moscow", "Madrid", "Stop", "Kyiv"),
//    3 -> List("Cat", "Stop", "Hamster", "Fish", "Turtle"),
    3 -> List("Stop", "Stop", "Stop", "Stop", "Stop"),
    4 -> List("Bread", "Milk", "Kefir", "Stop", "Beer"),
    5 -> List("Stop", "UCL", "Stop", "Cambridge", "Oxford"))


  val stoppableTestMap2 = testMap2.mapValues(_.map(s => if (s == "Stop") Stop else Data(s)))

  override protected def finalAssertions[A](learners: Seq[ActorRef], factory: PaxosFactory[A],
                                            res: Seq[MessageForSlot[Any]]) = {
    import factory._
    // assert(res.size == learners.size, s"heard back from all learners")
    assert(res.forall { case MessageForSlot(_, LearnedAgreedValue(v, l)) =>
      v == res.head.msg.asInstanceOf[LearnedAgreedValue].value
    }, s"All learners should return the same result at the end.")
  }


  override protected def receiveFromLearners(learners: Seq[ActorRef]): Seq[AnyRef] = {
    import scala.concurrent.duration._
    receiveWhile(1 second){case x => x}
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


class StoppablePaxosStops extends StoppableMultiPaxosTests {
  s"A Stoppable Paxos" must {
    s"cease to proceed for further slots once a stoppable command is issued" in {
      // A map from slots to values
      setupAndTestInstances(stoppableTestMap2, new StoppablePaxosFactory[String])
    }
  }
}