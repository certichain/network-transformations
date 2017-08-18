package org.protocols.register.util

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorRef
import org.protocols.register.{MessageToProxy, RegisterMessage}

/**
  * @author Ilya Sergey

  *         [Auxiliary Machinery], feel free to ignore
  *         Utility methods and auxiliary fields for round-based register communication.
  */

trait RegisterAux {

  val myProxy: ActorRef
  val contextParams: Any
  val quorumSize: Int

  private val myMailbox: ConcurrentLinkedQueue[Any] = new ConcurrentLinkedQueue[Any]()
  private val timeoutMillis = 100
  protected val self: ActorRef = myProxy // Middleman for virtualisation

  protected def emitMsg(msg: RegisterMessage): Unit = {
    self ! MessageToProxy(msg, contextParams)
  }

  def deliver(msg: Any): Unit = {
    myMailbox.synchronized {
      // Need to synchronize in order to avoid infinite loops with `processIncomingMessages`
      myMailbox.add(msg)
    }
  }

  /**
    * Processing the messages in the mailbox.
    * Resorting to shameful shared-memory concurrency... [sigh].
    *
    * @param f function to select and process messages
    */
  protected def processInbox(f: PartialFunction[Any, Unit]) {
    // Wait until enough messages received instead of timeout
    var shouldProcess = true

    // Loop while not sufficiently many mails collected
    while (shouldProcess) {
      myMailbox.synchronized {
        val iter = myMailbox.iterator()
        var inbox: Set[Any] = Set.empty
        while (iter.hasNext) {
          val msg = iter.next()
          // Check applicability and ignore duplicates
          if (f.isDefinedAt(msg) && !inbox.contains(msg)) {
            inbox = inbox + msg
          }
        }
        // Collected sufficiently many letters to process: can now shoot
        if (inbox.size >= quorumSize) {
          // Process all the collected incoming mails with f
          for (m <- inbox) {
            f(m)
          }
          // Clean the inbox from similar messages
          myMailbox.removeIf(f.isDefinedAt(_))
          shouldProcess = false
        }
      }
    }
  }

}


