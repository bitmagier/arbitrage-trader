package org.purevalue.arbitrage.trader

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.TradeRoom.{GetTradableAssets, TradableAssets, TradeRequestBundleCompleted, TradeRequestBundleFiled}
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.purevalue.arbitrage.{Main, TradeRequestBundle}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt


object FooTrader {
  case class Trigger()

  def props(config: Config, tradeRoom:ActorRef): Props = Props(new FooTrader(config, tradeRoom))
}

/**
 * A basic trader to evolve the concept
 */
class FooTrader(config: Config, tradeRoom:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[FooTrader])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val maxOpenTradeRequest: Int = config.getInt("max-open-trade-requests")
  var pendingTradeRequests:Set[TradeRequestBundle] = Set()
  var activeTradeRequests:Set[TradeRequestBundle] = Set()
  // TODO var completedTradeRequest:
  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(0.seconds, 1.second, self, Trigger())

  def findBestShot(t:TradableAssets): Option[TradeRequestBundle] = ???

  override def receive: Receive = {
    case Trigger =>
      if (pendingTradeRequests.size < maxOpenTradeRequest)
        tradeRoom ! GetTradableAssets()

    case t:TradableAssets =>
      findBestShot(t) match {
        case Some(shot) =>
          pendingTradeRequests = pendingTradeRequests + shot
          tradeRoom ! shot
      }

   case TradeRequestBundleFiled(t) =>
     pendingTradeRequests = pendingTradeRequests - t
     activeTradeRequests = activeTradeRequests + t

    case t:TradeRequestBundleCompleted =>
      activeTradeRequests = activeTradeRequests - t.request

    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
