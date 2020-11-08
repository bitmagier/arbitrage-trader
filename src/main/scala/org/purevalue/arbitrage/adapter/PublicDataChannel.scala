package org.purevalue.arbitrage.adapter

import java.util.concurrent.TimeUnit

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorSystem, Behavior, PostStop, Signal}
import org.purevalue.arbitrage.util.ConnectionClosedException
import org.purevalue.arbitrage.{ExchangeConfig, Main, UserRootGuardian}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object PublicDataChannel {
  trait Event
  case class OnStreamsRunning() extends Event
  case class DeliverTradePairStats() extends Event
  case class Disconnected() extends Event
}
abstract class PublicDataChannel(context: ActorContext[PublicDataChannel.Event],
                                 timers: TimerScheduler[PublicDataChannel.Event],
                                 exchangeConfig: ExchangeConfig) extends AbstractBehavior[PublicDataChannel.Event](context) {

  import PublicDataChannel._

  implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  implicit val executionContext: ExecutionContext = system.executionContext

  def onStreamsRunning(): Unit = {}

  def deliverTradePairStats(): Unit

  def postStop(): Unit = {}

  override def onMessage(message: Event): Behavior[Event] = message match {
    // @formatter:off
    case OnStreamsRunning()      => onStreamsRunning(); Behaviors.same
    case DeliverTradePairStats() => deliverTradePairStats(); Behaviors.same
    case Disconnected()          => throw new ConnectionClosedException(getClass.getSimpleName)
    // @formatter:on
  }

  override def onSignal: PartialFunction[Signal, Behavior[Event]] = {
    case PostStop => postStop(); Behaviors.same
  }

  timers.startTimerAtFixedRate(DeliverTradePairStats(), FiniteDuration(exchangeConfig.pullTradePairStatsInterval.toMillis, TimeUnit.MILLISECONDS))
}
