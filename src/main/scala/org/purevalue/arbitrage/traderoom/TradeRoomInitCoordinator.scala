package org.purevalue.arbitrage.traderoom

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Config
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataChannel, BinancePublicDataInquirer}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexAccountDataChannel, BitfinexPublicDataChannel, BitfinexPublicDataInquirer}
import org.purevalue.arbitrage.adapter.coinbase.{CoinbaseAccountDataChannel, CoinbasePublicDataChannel, CoinbasePublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradeRoom.{ConcurrentMap, OrderRef}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator.InitializedTradeRoom
import org.purevalue.arbitrage.traderoom.exchange.Exchange
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt


case class ExchangeInitStuff(exchangePublicDataInquirerProps: ExchangePublicDataInquirerInit,
                             exchangePublicDataChannelProps: ExchangePublicDataChannelInit,
                             exchangeAccountDataChannelProps: ExchangeAccountDataChannelInit)

object TradeRoomInitCoordinator {
  case class InitializedTradeRoom(tradeRoom: ActorRef)
  def props(config: Config,
            parent: ActorRef): Props = Props(new TradeRoomInitCoordinator(config, parent))
}
class TradeRoomInitCoordinator(val config: Config,
                               val parent: ActorRef) extends Actor {

  private val log = LoggerFactory.getLogger(classOf[TradeRoomInitCoordinator])

  // @formatter:off
  var allTradePairs:      Map[String, Set[TradePair]] = Map()
  var usableTradePairs:   Map[String, Set[TradePair]] = Map()
  var exchanges:          Map[String, ActorRef] = Map()
  var tickers:            Map[String, ConcurrentMap[TradePair, Ticker]] = Map()
  var orderBooks:         Map[String, ConcurrentMap[TradePair, OrderBook]] = Map()
  var dataAge:            Map[String, PublicDataTimestamps] = Map()
  var wallets:            Map[String, Wallet] = Map()
  var activeOrders:       Map[String, ConcurrentMap[OrderRef, Order]] = Map()
  // @formatter:on

  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      BinancePublicDataInquirer.props,
      BinancePublicDataChannel.props,
      BinanceAccountDataChannel.props
    ),
    "bitfinex" -> ExchangeInitStuff(
      BitfinexPublicDataInquirer.props,
      BitfinexPublicDataChannel.props,
      BitfinexAccountDataChannel.props
    ),
    "coinbase" -> ExchangeInitStuff(
      CoinbasePublicDataInquirer.props,
      CoinbasePublicDataChannel.props,
      CoinbaseAccountDataChannel.props
    )
  )

  def queryAllTradePairs(exchange: String): Set[TradePair] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    Await.result((exchanges(exchange) ? GetAllTradePairs()).mapTo[Set[TradePair]], timeout.duration.plus(500.millis))
  }

  def queryAllTradePairs(): Unit = {
    for (exchange <- exchanges.keys) {
      allTradePairs = allTradePairs + (exchange -> queryAllTradePairs(exchange))
    }
  }

  // we want to trade only trade pairs
  // - [1] which are NOT Fiat money
  // - [2] that are available on at least two exchanges
  // - [3] plus trade pairs, where one side is a part of [2] and the other side is a local reserve asset or USD equivalent coin
  def determineUsableTradepairs(): Unit = {
    val allGlobalTradePairs = allTradePairs.values.flatten.toSet
    val globalArbitragePairs = allGlobalTradePairs.filter(e => allTradePairs.count(_._2.contains(e)) > 1)

    val arbitrageAssets = globalArbitragePairs.flatMap(_.involvedAssets)
    def condition3(exchange: String, tp: TradePair): Boolean = {
      (arbitrageAssets.contains(tp.baseAsset) && (tp.quoteAsset == config.exchanges(exchange).usdEquivalentCoin || config.exchanges(exchange).reserveAssets.contains(tp.quoteAsset))) ||
        (arbitrageAssets.contains(tp.quoteAsset) && tp.baseAsset == config.exchanges(exchange).usdEquivalentCoin || config.exchanges(exchange).reserveAssets.contains(tp.baseAsset))
    }

    usableTradePairs = allTradePairs
      .map(e => e._1 -> e._2.filter(x => globalArbitragePairs.contains(x) || condition3(e._1, x)))
  }

  def pushUsableTradePairs(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    exchanges.foreach {
      case (exchange, actor) => Await.ready(actor ? SetUsableTradePairs(usableTradePairs(exchange)), timeout.duration.plus(500.millis))
    }
  }

  def startExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    tickers = tickers + (exchangeName -> TrieMap())
    orderBooks = orderBooks + (exchangeName -> TrieMap())
    dataAge = dataAge + (exchangeName -> PublicDataTimestamps(None, None, None))
    wallets = wallets + (exchangeName -> Wallet(exchangeName, Map(), config.exchanges(exchangeName)))
    activeOrders = activeOrders + (exchangeName -> TrieMap())

    exchanges = exchanges +
      (exchangeName -> context.actorOf(
        Exchange.props(
          exchangeName,
          config,
          config.exchanges(exchangeName),
          exchangeInit,
          ExchangePublicData(
            tickers(exchangeName),
            orderBooks(exchangeName),
            dataAge(exchangeName)
          ),
          ExchangeAccountData(
            wallets(exchangeName),
            activeOrders(exchangeName)
          )
        ), "Exchange-" + exchangeName))
  }

  def startExchanges(): Unit = {
    for (name: String <- config.exchanges.keys) {
      startExchange(name, AllExchanges(name))
    }
  }

  var exchangesStreamingPending: Set[String] = _

  def sendStartStreaming(): Unit = {
    exchangesStreamingPending = exchanges.keySet
    exchanges.values.foreach { exchange =>
      exchange ! StartStreaming()
    }
  }

  def onInitialized(): Unit = {
    log.debug("TradeRoom initialized")
    val tradeRoom = context.actorOf(TradeRoom.props(config, exchanges, usableTradePairs, tickers, orderBooks, dataAge, wallets, activeOrders), "TradeRoom")
    parent ! InitializedTradeRoom(tradeRoom)
    context.watch(tradeRoom)
  }

  def onStreamingStarted(exchange: String): Unit = {
    exchangesStreamingPending = exchangesStreamingPending - exchange
    if (exchangesStreamingPending.isEmpty) {
      onInitialized()
    }
  }

  override def preStart(): Unit = {
    startExchanges() // parallel
    queryAllTradePairs()
    determineUsableTradepairs()
    pushUsableTradePairs()
    sendStartStreaming()
  }

  override def receive: Receive = {
    case Exchange.StreamingStarted(exchange) => onStreamingStarted(exchange)
    case msg => log.error(s"unexpected message: $msg")
  }
}
