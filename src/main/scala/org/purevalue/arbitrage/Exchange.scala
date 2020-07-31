package org.purevalue.arbitrage
import akka.actor.{Actor, ActorRef, Props}
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.{GetTradePairs, TradePairs}
import org.slf4j.LoggerFactory

case class TradePair(symbol:String, baseAsset:String, quoteAsset:String)
case class Fee(makerFee:Double, takerFee:Double)

object Exchange {
  def props(name:String, config:ExchangeConfig, adapter:ActorRef): Props = Props(new Exchange(name, config, adapter))
}

case class Exchange(name:String, config:ExchangeConfig, exchangeAdapter:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])

  val assets:Set[String] = config.assets
  val fee: Fee = Fee(config.makerFee, config.takerFee)
  // dynamic
  var tradePairs: Set[TradePair] = _
  var orderBooks: Map[TradePair, ActorRef] = _
  var orderBookInitPending:Set[TradePair] = _
  private var _initialized: Boolean = false

  def initOrderBooks(): Unit = {
    orderBookInitPending = tradePairs
    orderBooks = Map[TradePair, ActorRef]()
    for (p <- tradePairs) {
      orderBooks += (p -> context.actorOf(OrderBook.props(name, p, exchangeAdapter, self), s"$name.OrderBook-${p.symbol}"))
    }
  }

  override def preStart(): Unit = {
    log.info(s"Initializing exchange $name")
    exchangeAdapter ! GetTradePairs
  }

  override def receive: Receive = {
    case TradePairs(t) =>
      tradePairs = t
      log.info(s"$name: ${tradePairs.size} TradePairs received: $tradePairs")
      initOrderBooks()

    case OrderBook.Initialized(t) =>
      orderBookInitPending -= t
      if (orderBookInitPending.isEmpty) {
        _initialized = true
        log.info(s"$name: all OrderBooks initialized")
      }
  }
}
