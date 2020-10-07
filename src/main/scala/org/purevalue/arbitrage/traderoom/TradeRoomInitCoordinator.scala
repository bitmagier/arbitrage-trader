package org.purevalue.arbitrage.traderoom

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Config
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataChannel, BinancePublicDataInquirer}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexAccountDataChannel, BitfinexPublicDataChannel, BitfinexPublicDataInquirer}
import org.purevalue.arbitrage.adapter.coinbase.{CoinbaseAccountDataChannel, CoinbasePublicDataChannel, CoinbasePublicDataInquirer}
import org.purevalue.arbitrage.traderoom.Asset.Bitcoin
import org.purevalue.arbitrage.traderoom.TradeRoom.{ConcurrentMap, OrderRef}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator.InitializedTradeRoom
import org.purevalue.arbitrage.traderoom.exchange.Exchange
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.util.Emoji
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
  var tickerTradePairs:   Map[String, Set[TradePair]] = Map()
  var tradableTradePairs: Map[String, Set[TradePair]] = Map()
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

  def queryTickerTradePairs(exchange: String): Set[TradePair] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    Await.result((exchanges(exchange) ? GetTickerTradePairs()).mapTo[Set[TradePair]], timeout.duration.plus(500.millis))
  }

  def queryFinalTickerTradePairs(): Unit = {
    for (exchange <- exchanges.keys) {
      tickerTradePairs = tickerTradePairs + (exchange -> queryTickerTradePairs(exchange))
    }
  }

  def dropTradePairSync(exchangeName: String, tp: TradePair): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    Await.ready(exchanges(exchangeName) ? RemoveTickerTradePair(tp), timeout.duration.plus(500.millis))
  }


  /**
   * First of all we drop tradepairs, where one part is a FIAT currency.
   *
   * Also we select none-reserve assets, where not at least two connected compatible TradePairs can be found looking at all exchanges.
   * Then we drop all TradePairs, connected to the selected ones.
   *
   * Reason: We need two TradePairs at least, to have one for the main-trdade and the other for the reserve-liquidity transaction.
   * (still we cannot be 100% sure, that every tried transaction can be fulfilled with providing liquidity via an uninvolved reserve-asset,
   * but we can increase the chances via that cleanup here)
   *
   * For instance we have an asset X (which is not one of the reserve assets), and the only tradable options are:
   * X:BTC & X:ETH on exchange1 and X:ETH on exchange2.
   * We remove both trade pairs, because there is only one compatible tradepair (X:ETH) which is available for arbitrage-trading and
   * the required liquidity cannot be provided on exchange2 with another tradepair (ETH does not work because it is involved in the trade)
   *
   * This method runs non-parallel and synchronously to finish together with all actions finished
   * Parallel optimization is possible but not necessary for this small task
   *
   * Note:
   * For liquidity conversion and some calculations we need USD equivalent  pairs in ReferenceTicker, so we don't drop x:USDT or x:USDC pairs
   * Also - if no x:USDT/USDC pair is available, we don't drop the x:BTC pair (like for IOTA on Bitfinex we only have IOTA:BTC & IOTA:ETH)
   */
  def dropUnusableTickerTradePairsSync(): Unit = {
    var eTradePairs: Set[Tuple2[String, TradePair]] = Set()
    for (exchange: String <- exchanges.keys) {
      val tp: Set[TradePair] = queryTickerTradePairs(exchange)
      eTradePairs = eTradePairs ++ tp.map(e => (exchange, e))
    }
    val cryptoAssetsToRemove: Set[Asset] =
      eTradePairs
        .filterNot(_._2.involvedAssets.exists(_.isFiat)) // we talk about non-FIAT tradepairs only here
        .map(_._2.baseAsset) // set of candidate assets
        .filterNot(e => config.exchanges.values.exists(_.reserveAssets.contains(e))) // don't select reserve assets
        .filterNot(a =>
          eTradePairs
            .filter(_._2.baseAsset == a) // all connected tradepairs X -> ...
            .groupBy(_._2.quoteAsset) // grouped by other side of TradePair (trade options)
            .count(e => e._2.size > 1) > 1 // tests, if at least two trade options exists (for our candidate base asset), that are present on at least two exchanges
        )

    val fiatTradePairs: Set[Tuple2[String, TradePair]] = eTradePairs.filter(_._2.involvedAssets.exists(_.isFiat))
    log.debug(s"${Emoji.Robot}  Dropping all FIAT trade pairs: $fiatTradePairs")
    for ((exchange,tp) <- fiatTradePairs) {
      dropTradePairSync(exchange, tp)
    }

    for (asset <- cryptoAssetsToRemove) {
      val tickerTradePairsToDrop: Set[Tuple2[String, TradePair]] =
        eTradePairs
          .filter(e => e._2.baseAsset == asset && e._2.quoteAsset != config.exchanges(e._1).usdEquivalentCoin) // keep :USD-equivalent TradePairs because we want them for currency calculations (and in the ReferenceTicker)
          .filter(e => e._2.baseAsset == asset && !config.exchanges(e._1).reserveAssets.contains(e._2.quoteAsset)) // also keep reserve asset connected pairs
          .filterNot(e =>
            !eTradePairs.exists(x => x._1 == e._1 && x._2 == TradePair(e._2.baseAsset, config.exchanges(e._1).usdEquivalentCoin)) && // when no :USD-eqiv tradepair exists
              e._2 == TradePair(e._2.baseAsset, Bitcoin)) // keep :BTC tradepair (for currency conversion via x -> BTC -> USD-equiv)

      if (tickerTradePairsToDrop.nonEmpty) {
        log.debug(s"${Emoji.Robot}  Dropping some trade pairs involving $asset, because we don't have a use for it:  $tickerTradePairsToDrop")
        tickerTradePairsToDrop.foreach(e => dropTradePairSync(e._1, e._2))
      }
    }
    log.info(s"${Emoji.Robot}  Finished cleanup of unusable trade pairs")
  }

  // we want to trade only trade pairs
  // - [1] that are available on at least two exchanges
  // - [2] plus tradpairs, where one side is a part of [1] and the other side is a local reserve asset or USD equivalent coin
  def determineTradableTradepairs(): Unit = {
    val allTradePairs = tickerTradePairs.values.flatten.toSet
    val arbitragePairs = allTradePairs.filter(e => tickerTradePairs.count(_._2.contains(e)) > 1)
    def condition2(exchange:String, tp:TradePair): Boolean = {
      val arbitrageAssets = arbitragePairs.flatMap(_.involvedAssets)
      arbitrageAssets.contains(tp.baseAsset) || arbitrageAssets.contains(tp.quoteAsset) ||
        tp.involvedAssets.contains(config.exchanges(exchange).usdEquivalentCoin) ||
        config.exchanges(exchange).reserveAssets.contains(tp.baseAsset) ||
        config.exchanges(exchange).reserveAssets.contains(tp.quoteAsset)
    }
    tradableTradePairs = tickerTradePairs.map(e => e._1 -> e._2.filter(x => arbitragePairs.contains(x) || condition2(e._1, x)))
  }

  def pushTradableTradePairs(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    exchanges.foreach {
      case (exchange,actor) => Await.ready(actor ? SetTradableTradePairs(tradableTradePairs(exchange)), timeout.duration.plus(500.millis))
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
    val tradeRoom = context.actorOf(TradeRoom.props(config, exchanges, tickerTradePairs, tradableTradePairs, tickers, orderBooks, dataAge, wallets, activeOrders), "TradeRoom")
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
    dropUnusableTickerTradePairsSync() // all exchanges, sync
    queryFinalTickerTradePairs()
    determineTradableTradepairs()
    pushTradableTradePairs()
    sendStartStreaming()
  }

  override def receive: Receive = {
    case Exchange.StreamingStarted(exchange) => onStreamingStarted(exchange)
    case msg => log.error(s"unexpected message: $msg")
  }
}
