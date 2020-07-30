package org.purevalue.arbitrage
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.adapter.BinanceAdapter
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.{GetTradePairs, TradePairs}

case class TradePair(symbol:String, baseAsset:String, quoteAsset:String)
case class Fee(makerFee:Double, takerFee:Double)

abstract class Exchange extends Actor {
  private val log = Logging(context.system, this)
  // static
  def name:String
  def assets:Set[String]
  private val orderBookInitTimeout = java.time.Duration.ofSeconds(15)
  def fee:Fee
  // dynamic
  var tradePairs: Set[TradePair] = _
  var orderBooks: Map[TradePair, ActorRef] = _
  var orderBookInitPending:Set[TradePair] = _
  private var _initialized: Boolean = false
  def initialized: Boolean = _initialized

  def exchangeAdapter:ActorRef

  def initOrderBooks(): Unit = {
    orderBookInitPending = tradePairs
    orderBooks = Map[TradePair, ActorRef]()
    for (p <- tradePairs) {
      orderBooks += (p -> context.actorOf(OrderBook.props(name, p, exchangeAdapter, self), s"$name.OrderBook-$p"))
    }
  }

  override def receive: Receive = {
    case TradePairs(t) =>
      tradePairs = t
      log.info(s"$name TradePairs initialized")
      initOrderBooks()

    case OrderBook.Initialized(t) =>
      orderBookInitPending -= t
      if (orderBookInitPending.isEmpty) {
        _initialized = true
      }
  }

  override def postStop(): Unit = {
    super.postStop()
    orderBooks.values.foreach(e => actorSystem.stop(e))
  }
}

object Binance {
  def props(config:ExchangeConfig): Props = Props(new Binance(config))
}

class Binance(val config:ExchangeConfig) extends Exchange {
  private val log = Logging(context.system, this)
  val name = "binance"
  val assets: Set[String] = config.assets
  val fee: Fee = Fee(config.makerFee, config.takerFee)

  var exchangeAdapter: ActorRef = _

  override def preStart(): Unit = {
    super.preStart()
    log.info(s"Initializing exchange $name")
    exchangeAdapter = context.actorOf(Props[BinanceAdapter], "BinanceAdapter")
    exchangeAdapter ! GetTradePairs
  }

  override def postStop(): Unit = {
    super.postStop()
    actorSystem.stop(exchangeAdapter)
  }
}