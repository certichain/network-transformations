package org.protocols.register

import akka.actor.ActorRef

/**
  * Register-based messages
  */
abstract sealed class RegisterMessage {
  // Source of the message
  def src: ActorRef
  // An actor to send this message to
  def dest: ActorRef
  // Ballot, subject of interaction
  def k: Int
}

final case class READ(src: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage
final case class ackREAD(src: ActorRef, dest: ActorRef, k: Int, kWv: Option[(Int, Any)]) extends RegisterMessage
final case class nackREAD(src: ActorRef, dest: ActorRef, k: Int, kWv: Option[(Int, Any)]) extends RegisterMessage

final case class WRITE(src: ActorRef, dest: ActorRef, k: Int, vW: Any) extends RegisterMessage
final case class ackWRITE(src: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage
final case class nackWRITE(src: ActorRef, dest: ActorRef, k: Int) extends RegisterMessage

case class MessageToProxy(rm: RegisterMessage, params: Any)

