package org.purevalue.arbitrage.trader

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.TradeRoom.{GetTradeblePairs, TradeRequestFiled, TradeblePairs}
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.purevalue.arbitrage.{Main, TradeRequestSet}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt


object FooTrader {
  case class Trigger()

  def props(config: Config, tradeRoom:ActorRef): Props = Props(new FooTrader(config, tradeRoom))
}

class FooTrader(config: Config, tradeRoom:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[FooTrader])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val maxOpenTradeRequest: Int = config.getInt("max-open-trade-requests")
  var pendingTradeRequests:Set[TradeRequestSet] = Set()
  var activeTradeRequests:Set[TradeRequestSet] = Set()
  // TODO var completedTradeRequest:
  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(0.seconds, 1.second, self, Trigger())

  def findBestShot(t:TradeblePairs): Option[TradeRequestSet] = ???

  override def receive: Receive = {
    case Trigger() =>
      if (pendingTradeRequests.size < maxOpenTradeRequest)
        tradeRoom ! GetTradeblePairs()

    case t:TradeblePairs =>
      val shot:Option[TradeRequestSet] = findBestShot(t)
      if (shot.isDefined) {
        pendingTradeRequests = pendingTradeRequests ++ shot
        tradeRoom ! shot
      }

   case TradeRequestFiled(t) =>
     pendingTradeRequests = pendingTradeRequests.filterNot(_ == t)
     activeTradeRequests = activeTradeRequests + t
//    case TradeExecuted() =>


    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
