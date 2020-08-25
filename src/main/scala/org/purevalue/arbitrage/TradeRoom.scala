package org.purevalue.arbitrage

import java.time.{Duration, Instant, LocalDateTime, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.Exchange.{GetTradePairs, RemoveRunningTradePair, RemoveTradePair, StartStreaming}
import org.purevalue.arbitrage.TradeRoom.{LogStats, OrderBundle, TradeContext}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.purevalue.arbitrage.trader.FooTrader
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor}

sealed trait TradeSide
object TradeSide extends TradeSide {
  case object Buy extends TradeSide
  case object Sell extends TradeSide
}

case class CryptoValue(asset: Asset, amount: Double) {
  override def toString: String = s"${formatDecimal(amount)} ${asset.officialSymbol}"

  def convertTo(targetAsset: Asset, findConversionRate: TradePair => Option[Double]): Option[CryptoValue] = {
    if (this.asset == targetAsset)
      Some(this)
    else {
      // try direct conversion first
      findConversionRate(TradePair.of(this.asset, targetAsset)) match {
        case Some(rate) =>
          Some(CryptoValue(targetAsset, amount * rate))
        case None =>
          findConversionRate(TradePair.of(targetAsset, this.asset)) match { // try reverse ticker
            case Some(rate) =>
              Some(CryptoValue(targetAsset, amount / rate))
            case None => // try conversion via BTC as last option
              None
              if ((this.asset != Bitcoin && targetAsset != Bitcoin)
                && findConversionRate(TradePair.of(this.asset, Bitcoin)).isDefined
                && findConversionRate(TradePair.of(targetAsset, Bitcoin)).isDefined) {
                this.convertTo(Bitcoin, findConversionRate).get.convertTo(targetAsset, findConversionRate)
              } else {
                None
              }
          }
      }
    }
  }

  def convertTo(targetAsset: Asset, tc: TradeContext): Option[CryptoValue] =
    convertTo(targetAsset, tp => tc.findReferenceTicker(tp).map(_.weightedAveragePrice))
}
/**
 * CryptoValue on a specific exchange
 */
case class LocalCryptoValue(exchange: String, asset: Asset, amount: Double)

/** Order: a single trade request before it's execution */
case class Order(id: UUID,
                 orderBundleId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 tradeSide: TradeSide,
                 fee: Fee,
                 amountBaseAsset: Double,
                 limit: Double) {
  override def toString: String = s"Order($id, orderBundleId:$orderBundleId, $exchange, $tradePair, $tradeSide, $fee, " +
    s"amountBaseAsset:${formatDecimal(amountBaseAsset)}, limit:${formatDecimal(limit)})"

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset * (1.0 + fee.takerFee))
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset)
    case _ => throw new IllegalArgumentException
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset)
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset * (1.0 - fee.takerFee))
    case _ => throw new IllegalArgumentException
  }
}

/** Trade: a successfully executed Order */
case class Trade(id: UUID,
                 orderId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 direction: TradeSide,
                 fee: Fee,
                 amountBaseAsset: Double,
                 amountQuoteAsset: Double,
                 executionRate: Double,
                 executionTime: ZonedDateTime,
                 wasMaker: Boolean,
                 supervisorComments: Seq[String]) // comments from TradeRoom "operator"

case class OrderBill(balanceSheet: Seq[CryptoValue], sumUSDT: Double) {
  override def toString: String = s"OrderBill(balanceSheet:$balanceSheet, sumUSDT:${formatDecimal(sumUSDT)})"
}
object OrderBill {
  /**
   * Calculates the balance sheet of that order
   * incoming value have positive amount; outgoing value have negative amount
   */
  def calcBalanceSheet(order: Order): Seq[CryptoValue] = {
    val result = ArrayBuffer[CryptoValue]()
    order.calcIncomingLiquidity match {
      case v: LocalCryptoValue => result.append(CryptoValue(v.asset, v.amount))
    }
    order.calcOutgoingLiquidity match {
      case v: LocalCryptoValue => result.append(CryptoValue(v.asset, -v.amount))
    }
    result
  }

  def aggregateValues(balanceSheet: Iterable[CryptoValue], targetAsset: Asset, findReferenceRate: TradePair => Option[Double]): Double = {
    val sumByAsset: Iterable[CryptoValue] = balanceSheet
      .groupBy(_.asset)
      .map(e => CryptoValue(e._1, e._2.map(_.amount).sum))

    sumByAsset.map { v =>
      v.convertTo(targetAsset, findReferenceRate) match {
        case Some(v) => v.amount
        case None =>
          throw new RuntimeException(s"Unable to convert ${v.asset} to $targetAsset")
      }
    }.sum
  }

  def aggregateValues(balanceSheet: Iterable[CryptoValue], targetAsset: Asset, tc: TradeContext): Double =
    aggregateValues(balanceSheet, targetAsset, tp => tc.findReferenceTicker(tp).map(_.weightedAveragePrice))

  def calc(orders: Seq[Order], tc: TradeContext): OrderBill = {
    val balanceSheet: Seq[CryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSDT: Double = aggregateValues(balanceSheet, Asset("USDT"), tc)
    OrderBill(balanceSheet, sumUSDT)
  }
}


object TradeRoom {

  /**
   * An always-uptodate view on the TradeRoom Pre-Trade Data.
   * Modification of the content is NOT permitted by users of the TRadeContext (even if technically possible)!
   */
  case class TradeContext(tickers: scala.collection.Map[String, scala.collection.Map[TradePair, Ticker]],
                          extendedTickers: scala.collection.Map[String, scala.collection.Map[TradePair, ExtendedTicker]],
                          orderBooks: scala.collection.Map[String, scala.collection.Map[TradePair, OrderBook]],
                          balances: scala.collection.Map[String, Wallet],
                          fees: scala.collection.Map[String, Fee]) {

    def findReferenceTicker(tp: TradePair): Option[ExtendedTicker] = TradeRoom.findReferenceTicker(tp, extendedTickers)
  }

  /** High level order bundle, covering 2 or more trader orders */
  case class OrderBundle(id: UUID,
                         traderName: String,
                         trader: ActorRef,
                         creationTime: LocalDateTime,
                         orders: Seq[Order],
                         bill: OrderBill) {
    override def toString: String = s"OrderBundle($id, $traderName, creationTime:$creationTime, orders:$orders, $bill)"
  }

  // communication with ourselfs
  case class LogStats()


  /**
   * find the best ticker stats for the tradepair prioritized by exchange via config
   */
  def findReferenceTicker(tradePair: TradePair, extendedTickers: scala.collection.Map[String, scala.collection.Map[TradePair, ExtendedTicker]]): Option[ExtendedTicker] = {
    for (exchange <- AppConfig.tradeRoom.extendedTickerExchanges) {
      extendedTickers.get(exchange) match {
        case Some(eTickers) => eTickers.get(tradePair) match {
          case Some(ticker) => return Some(ticker)
          case _ =>
        }
        case _ =>
      }
    }
    None
  }

  def props(config: TradeRoomConfig): Props = Props(new TradeRoom(config))
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom(config: TradeRoomConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private var exchanges: Map[String, ActorRef] = Map()
  private var initializedExchanges: Set[String] = Set()
  private var traders: Map[String, ActorRef] = Map()

  // a map per exchange
  type ConcurrentMap[A, B] = scala.collection.concurrent.Map[A, B]
  private val tickers: ConcurrentMap[String, ConcurrentMap[TradePair, Ticker]] = TrieMap()
  private val extendedTickers: ConcurrentMap[String, ConcurrentMap[TradePair, ExtendedTicker]] = TrieMap()
  private val orderBooks: ConcurrentMap[String, ConcurrentMap[TradePair, OrderBook]] = TrieMap()
  private val wallets: ConcurrentMap[String, Wallet] = TrieMap()
  private val dataAge: ConcurrentMap[String, TPDataTimestamps] = TrieMap()

  private val fees: Map[String, Fee] = Map( // TODO
    "binance" -> Fee("binance", AppConfig.exchange("binance").makerFee, AppConfig.exchange("binance").takerFee),
    "bitfinex" -> Fee("bitfinex", AppConfig.exchange("bitfinex").makerFee, AppConfig.exchange("bitfinex").takerFee)
  )

  private val tradeContext: TradeContext = TradeContext(tickers, extendedTickers, orderBooks, wallets, fees)

  private val orderBundleSafetyGuard = OrderBundleSafetyGuard(config.orderBundleSafetyGuard, tickers, extendedTickers, dataAge)

  val scheduleRate: FiniteDuration = FiniteDuration(config.statsInterval.toNanos, TimeUnit.NANOSECONDS)
  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(30.seconds, scheduleRate, self, LogStats())


  def checkAndLockRequiredLiquidity(values: Iterable[LocalCryptoValue]): Boolean = {
    true // TODO ask liquidity manager to lock that amount of CryptoValue on the specified exchanges for us
  }

  def placeOrderBundleOrders(t: OrderBundle): Unit = {
    if (orderBundleSafetyGuard.isSafe(t)) {
      val requiredLiquidity: Seq[LocalCryptoValue] = t.orders.map(_.calcOutgoingLiquidity)
      if (checkAndLockRequiredLiquidity(requiredLiquidity)) {
        log.info(s"${Emoji.Excited}  [simulated] Placing checked $t")
        // TODO don't forget to unlock requested liquidity after order is completed
      } else {
        log.info(s"${Emoji.Robot}  [simulated] Requesting liquidity for $t: $requiredLiquidity")
      }
    }
  }


  def logStats(): Unit = {
    def toEntriesPerExchange[T](m: scala.collection.Map[String, scala.collection.Map[TradePair, T]]): String = {
      m.map(e => (e._1, e._2.values.size))
        .toSeq
        .sortBy(_._1)
        .map(e => s"${e._1}:${e._2}")
        .mkString(", ")
    }

    val liquidityPerExchange: String =
      wallets.map { case (exchange, b) => (
        exchange,
        CryptoValue(
          Bitcoin, // conversion to BTC is expected to work ALWAYS!
          b.balances
            .map(e => CryptoValue(e._2.asset, e._2.amountAvailable).convertTo(Bitcoin, tradeContext).get)
            .map(_.amount)
            .sum
        ))
      }.map(e => s"${e._1}: ${e._2}").mkString(", ")
    log.info(s"${Emoji.Robot}  Available liquidity sums: ${liquidityPerExchange}")

    val freshestTicker = dataAge.maxBy(_._2.tickerTS.toEpochMilli)
    val oldestTicker = dataAge.minBy(_._2.tickerTS.toEpochMilli)
    log.info(s"${Emoji.Robot}  TradeRoom stats: [general] " +
      s"ticker:[${toEntriesPerExchange(tickers)}]" +
      s" (oldest: ${oldestTicker._1} ${Duration.between(oldestTicker._2.tickerTS, Instant.now).toMillis} ms," +
      s" freshest: ${freshestTicker._1} ${Duration.between(freshestTicker._2.tickerTS, Instant.now).toMillis} ms)" +
      s" / ExtendedTicker:[${toEntriesPerExchange(extendedTickers)}]" +
      s" / OrderBooks:[${toEntriesPerExchange(orderBooks)}]")
    if (config.orderBooksEnabled) {
      val orderBookTop3 = orderBooks.flatMap(_._2.values)
        .map(e => (e.bids.size + e.asks.size, e))
        .toSeq
        .sortBy(_._1)
        .reverse
        .take(3)
        .map(e => s"[${e._2.bids.size} bids/${e._2.asks.size} asks: ${e._2.exchange}:${e._2.tradePair}] ")
        .toList
      log.info(s"${Emoji.Robot}  TradeRoom stats: [biggest 3 OrderBooks] : $orderBookTop3")
      val orderBookBottom3 = orderBooks.flatMap(_._2.values)
        .map(e => (e.bids.size + e.asks.size, e))
        .toSeq
        .sortBy(_._1)
        .take(3)
        .map(e => s"[${e._2.bids.size} bids/${e._2.asks.size} asks: ${e._2.exchange}:${e._2.tradePair}] ")
        .toList
      log.info(s"${Emoji.Robot}  TradeRoom stats: [smallest 3 OrderBooks] : $orderBookBottom3")
    }
  }


  def removeTradePairSync(exchangeName: String, tp: TradePair): Unit = {
    implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
    Await.result(exchanges(exchangeName) ? RemoveTradePair(tp), timeout.duration)
  }

  /**
   * Here we remove non-reserve-asset-tradepairs which can only be traded against BTC or another single (reserve) asset,
   * because it is impossible for them to be provided via another reserve-asset than the one in a desired trade.
   * (see ExchangeLiquidityManager concept)
   *
   * This method runs non-parallel and synchronously to finish together with all actions finished
   * Parallel optimization is possible but not necessary for this small task
   */
  def removeSingleConversionOptionOnlyTradePairsSync(): Unit = {
    implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
    for (e: String <- exchanges.keys) {
      val tradePairs: Set[TradePair] = Await.result((exchanges(e) ? GetTradePairs()).mapTo[Set[TradePair]], timeout.duration)
      val toRemove: Set[TradePair] =
        tradePairs
          .filterNot(tp => AppConfig.liquidityManager.reserveAssets.contains(tp.baseAsset)) // don't select reserve-assets
          .filterNot(tp => tradePairs.count(_.baseAsset == tp.baseAsset) > 1) // don't select assets with multiple conversion options
          .filterNot(_.quoteAsset == USDT) // never remove X:USDT (required for conversion calculations)
      for (tp <- toRemove) {
        log.debug(s"${Emoji.Robot}  Removing TradePair $tp on $e because there we have only a single trade option with that base asset")
        removeTradePairSync(e, tp)
      }
    }
  }


  def removeTradePairsListedOnlyAtASingleExchangeSync(): Unit = {
    var tpExchanges: Map[TradePair, Set[String]] = Map()
    implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
    for (e: String <- exchanges.keys) {
      Await.result((exchanges(e) ? GetTradePairs()).mapTo[Set[TradePair]], timeout.duration).foreach { tp =>
        tpExchanges = tpExchanges + (tp -> (tpExchanges.getOrElse(tp, Set()) + e))
      }
    }
    tpExchanges // map[TradePair -> Set[exchanges]]
      .filter(_._2.size == 1) // select TP listed only on a single exchange
      .filterNot(_._1.quoteAsset == USDT) // never remove X:USDT (required for conversion calculations)
      .foreach { tpe =>
        val tp = tpe._1
        val e = tpe._2.head
        log.debug(s"${Emoji.Robot}  Removing TradePair $tp on $e because it is listed only on that exchange and nowhere else")
        removeTradePairSync(e, tp)
      }
  }

  def runExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    val camelName = exchangeName.substring(0, 1).toUpperCase + exchangeName.substring(1)
    tickers += exchangeName -> TrieMap[TradePair, Ticker]()
    extendedTickers += exchangeName -> TrieMap[TradePair, ExtendedTicker]()
    orderBooks += exchangeName -> TrieMap[TradePair, OrderBook]()
    dataAge += exchangeName -> TPDataTimestamps(Instant.MIN, Instant.MIN, Instant.MIN)

    wallets += exchangeName -> Wallet(Map())

    exchanges += exchangeName -> context.actorOf(
      Exchange.props(
        exchangeName,
        AppConfig.exchange(exchangeName),
        self,
        context.actorOf(exchangeInit.dataChannelProps.apply(), s"$camelName-Exchange"),
        exchangeInit.tpDataChannelProps,
        ExchangeTPData(
          tickers(exchangeName),
          extendedTickers(exchangeName),
          orderBooks(exchangeName),
          dataAge(exchangeName)
        ),
        ExchangeAccountData(
          wallets(exchangeName)
        )
      ), camelName)
  }

  def startExchanges(): Unit = {
    for (name: String <- AppConfig.activeExchanges) {
      runExchange(name, GlobalConfig.AllExchanges(name))
    }
  }

  def startTraders(): Unit = {
    traders += "FooTrader" -> context.actorOf(
      FooTrader.props(AppConfig.trader("foo-trader"), self, tradeContext),
      "FooTrader")
  }

  def cleanupTradePairs(): Unit = {
    removeSingleConversionOptionOnlyTradePairsSync()
    removeTradePairsListedOnlyAtASingleExchangeSync()
    log.info(s"${Emoji.Robot}  Finished cleanup of unusable trade pairs")
  }

  def startStreaming(): Unit = {
    for (exchange:ActorRef <- exchanges.values) {
      exchange ! StartStreaming()
    }
  }

  override def preStart(): Unit = {
    startExchanges()
    cleanupTradePairs()
    startStreaming()
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  def receive: Receive = {

    // messages from Exchanges
    case Exchange.Initialized(exchange) =>
      initializedExchanges = initializedExchanges + exchange
      if (exchanges.keySet == initializedExchanges) {
        log.info(s"${Emoji.Satisfied}  All exchanges initialized")
        self ! LogStats()
        startTraders()
      }

    // messages from Traders

    case ob: OrderBundle =>
      placeOrderBundleOrders(ob)

    // messages from outself

    case LogStats() =>
      if (exchanges.keySet == initializedExchanges)
        logStats()

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO restart whole exchange channels, when the ticker is aged (3 minutes)
// TODO shudown app in case of serious exceptions
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
