package org.purevalue.arbitrage

import java.time.ZonedDateTime

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.TradeRoom.GetTradeblePairs
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexAdapter
import org.purevalue.arbitrage.trader.FooTrader
import org.slf4j.LoggerFactory

trait TradeDirection
case class Buy() extends TradeDirection
case class Sell() extends TradeDirection

case class CryptoValue(asset: Asset, amount: Double)

case class SingleTradeRequest(exchange: String,
                              tradePair: TradePair,
                              direction: TradeDirection,
                              fee: Fee,
                              amountBaseAsset: Double,
                              amountQuoteAsset: Double,
                              limit: Double)
/** High level Trade Request from trader covering at least 2 SingleTradeRequests */
case class TradeRequest(id: String, creationTime:ZonedDateTime, requests: List[SingleTradeRequest], calculatedEarning: CryptoValue)

case class ExecutionDetails(executionTime:ZonedDateTime)
case class ExecutedTrade(request:TradeRequest, executionDetails:ExecutionDetails)

object TradeRoom {
  case class GetTradeblePairs()
  case class TradeblePairs(tradePair: TradePair, orderBooksByExchange: Map[String, OrderBook])
  case class TradeRequestFiled(request: TradeRequest)
  case class TradeCompletelyExecuted(request: TradeRequest, executedTrades:List[ExecutedTrade], earning:CryptoValue)

  def props(): Props = Props(new TradeRoom())
}
/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level view (per trade-request) of trades to traders
 */
class TradeRoom extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])

  var exchanges: Map[String, ActorRef] = Map()
  var traders: Map[String, ActorRef] = Map()

  override def preStart(): Unit = {
    exchanges += "binance" -> context.actorOf(Exchange.props("binance", StaticConfig.exchange("binance"),
      context.actorOf(BinanceAdapter.props(StaticConfig.exchange("binance")), "BinanceAdapter")), "binance")

    exchanges += "bitfinex" -> context.actorOf(Exchange.props("bitfinex", StaticConfig.exchange("bitfinex"),
      context.actorOf(BitfinexAdapter.props(StaticConfig.exchange("bitfinex")), "BitfinexAdapter")), "bitfinex")

    log.info(s"Initializing exchanges: ${exchanges.keys}")

//    traders += "foo" -> context.actorOf(FooTrader.props(StaticConfig.trader("foo")), "foo-trader")
  }

  def receive: Receive = {
    case GetTradeblePairs() => // TODO deliver TradeblePairs to sender

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO shudown app in case of exception from any actor
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
// TODO check order books of opposide trade direction - assure we have exactly one order book per exchange and 2 assets