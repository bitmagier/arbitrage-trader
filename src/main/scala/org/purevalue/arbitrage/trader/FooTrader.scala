package org.purevalue.arbitrage.trader

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.TradeRoom.{GetTradeblePairs, TradeRequestFiled, TradeblePairs}
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.purevalue.arbitrage.{Main, TradeRequest}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt


object FooTrader {
  case class Trigger()

  def props(config: Config, tradeRoom:ActorRef): Props = Props(new FooTrader(config, tradeRoom))
}

class FooTrader(config: Config, tradeRoom:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[FooTrader])
  implicit val system: ActorSystem = Main.actorSystem
  import system.dispatcher

  val maxOpenTradeRequest: Int = config.getInt("max-open-trade-requests")
  var pendingTradeRequests:List[TradeRequest] = List()
  var activeTradeRequests:List[TradeRequest] = List()
  // TODO var completedTradeRequest:
  val schedule: Cancellable = system.scheduler.scheduleAtFixedRate(0.seconds, 1.second, self, Trigger())

  def findBestShot(t:TradeblePairs): List[TradeRequest] = ???

  override def receive: Receive = {
    case Trigger() =>
      if (pendingTradeRequests.size < maxOpenTradeRequest) tradeRoom ! GetTradeblePairs()

    case t:TradeblePairs =>
      val shot:List[TradeRequest] = findBestShot(t)
      if (shot.nonEmpty) {
        pendingTradeRequests = shot ++ pendingTradeRequests
        tradeRoom ! shot
      }

   case TradeRequestFiled(t) =>
     pendingTradeRequests = pendingTradeRequests.filterNot(_ == t)
     activeTradeRequests = t :: activeTradeRequests
//    case TradeExecuted() =>


    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
