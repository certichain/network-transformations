package org.protocols.consensus23


/**
 * @author Ilya Sergey
 */

/*
class Consensus23Tests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("Consensus23Tests"))

  override def afterAll() {
    system.shutdown()
  }

  s"all nodes" must {
/*
    s"agree on the same value when is in 2/3-majority (Some(5))" in {
      val values = List(1, 2, 5, 5, 5, 5, 5, 5, 4)
      setupAndTestRound(0, values, Some(5))
    }
*/

    s"agree on the same non-taken value" in {
      val values = List(1, 5, 5, 4)
      setupAndTestRound(1, values, None)
    }

  }


  def setupAndTestRound[A](round: Int, values: List[A], expectedResult: Option[A]): Unit = {
    val num = values.size
    println(s"Round: $round, Number of nodes: $num")

    val nodes: Seq[ActorRef] = generateNodes(round, values)

    // Start the round
    for (node <- nodes) node ! DoSend(round, nodes)

    Thread.sleep(200)

    // ask the nodes for the result, so they would respond to the test node
    for (node <- nodes) node ! DoAsk(round, self)

    Thread.sleep(200)

    val res = receiveN(nodes.size, 30 seconds).asInstanceOf[Seq[DoTell[A]]]

    for (DoTell(r, vopt, id) <- res) {
      assert(r == round, s"Wrong round: $r received when $round expected")
      println(s"At the round $round, actor $id responds with the stored value $vopt")
      assert(vopt == expectedResult, s"the result of consensus should be $expectedResult")
    }
    println()
    afterAll()
  }

  def generateNodes[A](round: Int, values: List[A]): IndexedSeq[ActorRef] =
    for (i <- values.indices) yield {
      val v = values(i)
      system.actorOf(Props(classOf[Consensus23Node[Int]], v, round), name = s"round-$round-node-$i-offering-$v")
    }
}
*/
