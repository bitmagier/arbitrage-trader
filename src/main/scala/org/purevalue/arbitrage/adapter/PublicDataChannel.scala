package org.purevalue.arbitrage.adapter

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior, PostStop, Signal}
import org.purevalue.arbitrage.util.ConnectionClosedException
import org.purevalue.arbitrage.{Main, UserRootGuardian}

import scala.concurrent.ExecutionContext

object PublicDataChannel {
  trait Event
  case class OnStreamsRunning() extends Event()
  case class Disconnected() extends Event()
}
abstract class PublicDataChannel(context: ActorContext[PublicDataChannel.Event]) extends AbstractBehavior[PublicDataChannel.Event](context) {

  import PublicDataChannel._

  implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  implicit val executionContext: ExecutionContext = system.executionContext

  def onStreamsRunning(): Unit = {}

  def postStop(): Unit = {}

  override def onMessage(message: Event): Behavior[Event] = message match {
    // @formatter:off
    case OnStreamsRunning() => onStreamsRunning(); Behaviors.unhandled
    case Disconnected()     => throw new ConnectionClosedException(getClass.getSimpleName)
    // @formatter:on
  }

  override def onSignal: PartialFunction[Signal, Behavior[Event]] = {
    case PostStop => postStop(); Behaviors.same
  }
}
