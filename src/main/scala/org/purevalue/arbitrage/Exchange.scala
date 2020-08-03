package org.purevalue.arbitrage

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.{GetTradePairs, TradePairs}
import org.slf4j.LoggerFactory


case class Wallet(asset: Asset, amountAvailable: Double)
case class Fee(makerFee: Double, takerFee: Double)


object Exchange {
  def props(name: String, config: ExchangeConfig, adapter: ActorRef): Props = Props(new Exchange(name, config, adapter))
}

case class Exchange(name: String, config: ExchangeConfig, exchangeAdapter: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])

  val assets: Set[Asset] = GlobalConfig.assets.filter(e => config.assets.contains(e.officialSymbol))
  val fee: Fee = Fee(config.makerFee, config.takerFee)
  // dynamic
  var tradePairs: Set[TradePair] = _
  var orderBooks: Map[TradePair, ActorRef] = _
  var orderBookInitPending: Set[TradePair] = _
  var wallets: Map[Asset, Wallet] = Map( // TODO
    Asset("BTC") -> Wallet(Asset("BTC"), 0.5),
    Asset("USDT") -> Wallet(Asset("USDT"), 2000.0),
    Asset("ETH") -> Wallet(Asset("ETH"), 10.0),
    Asset("ADA") -> Wallet(Asset("ADA"), 0.0),
    Asset("ERD") -> Wallet(Asset("ERD"), 100000.0),
    Asset("ALGO") -> Wallet(Asset("ALGO"), 50000.0),
    Asset("BTG") -> Wallet(Asset("BTG"), 500.0)
  )

  private var _initialized: Boolean = false


  def initOrderBooks(): Unit = {
    orderBookInitPending = tradePairs
    orderBooks = Map[TradePair, ActorRef]()
    for (p <- tradePairs) {
      orderBooks += (p -> context.actorOf(OrderBookManager.props(name, p, exchangeAdapter, self), s"$name.OrderBook-${p.baseAsset.officialSymbol}-${p.quoteAsset.officialSymbol}"))
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

    case OrderBookManager.Initialized(t) =>
      log.info(s"$name: OrderBook $t initialized")
      orderBookInitPending -= t
      if (orderBookInitPending.isEmpty) {
        _initialized = true
        log.info(s"$name: all OrderBooks initialized and running")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO later: query fee dynamically