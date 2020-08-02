package org.purevalue.arbitrage

import akka.actor
import akka.actor.{Actor, Props, Status}
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexAdapter
import org.slf4j.LoggerFactory



trait TradeDirection
case class Buy() extends TradeDirection
case class Sell() extends TradeDirection

case class TradeRequest(exchange:String, tradePair: TradePair, direction:TradeDirection, fee:Fee, amountBaseAsset:Double, amountQuoteAsset:Double, limit:Double)

//case class ExecutedTrade(tradeRequest: TradeRequest, amountBaseAsset:Double, amountQuoteAsset:Double, rate:Double, fee:Fee)
//case class ExecutedTradeRequest(request:TradeRequest, trades:Seq[ExecutedTrade])


object TradeRoom {
  def props(): Props = Props(new TradeRoom())
}

class TradeRoom extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])

  var allExchanges: Map[String, actor.ActorRef] = _


  override def preStart(): Unit = {
    allExchanges = Map(
      "binance" -> context.actorOf(Exchange.props("binance", StaticConfig.exchange("binance"),
        context.actorOf(BinanceAdapter.props(StaticConfig.exchange("binance")), "BinanceAdapter")), "binance"
      ),
      "bitfinex" -> context.actorOf(Exchange.props("bitfinex", StaticConfig.exchange("bitfinex"),
        context.actorOf(BitfinexAdapter.props(StaticConfig.exchange("bitfinex")), "BitfinexAdapter")), "bitfinex")
    )
    log.info(s"Initializing exchanges: ${allExchanges.keys}")
  }

  def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
// TODO shudown app in case of exception from any actor
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
// TODO check order books of opposide trade direction - assure we have exactly one order book per exchange and 2 assets