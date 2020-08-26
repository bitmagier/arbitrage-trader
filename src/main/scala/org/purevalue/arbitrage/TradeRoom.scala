package org.purevalue.arbitrage

import java.time.{Duration, Instant, LocalDateTime, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.Exchange.{GetTradePairs, RemoveTradePair, StartStreaming}
import org.purevalue.arbitrage.TradeRoom.{DeathWatch, LogStats, OrderBundle, TradeContext}
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
  case class DeathWatch()

  /**
   * find the best ticker stats for the tradepair prioritized by exchange via config
   */
  def findReferenceTicker(tradePair: TradePair, extendedTickers: scala.collection.Map[String, scala.collection.Map[TradePair, ExtendedTicker]]): Option[ExtendedTicker] = {
    for (exchange <- Config.tradeRoom.extendedTickerExchanges) {
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
    "binance" -> Fee("binance", Config.exchange("binance").makerFee, Config.exchange("binance").takerFee),
    "bitfinex" -> Fee("bitfinex", Config.exchange("bitfinex").makerFee, Config.exchange("bitfinex").takerFee)
  )

  private val tradeContext: TradeContext = TradeContext(tickers, extendedTickers, orderBooks, wallets, fees)

  private val orderBundleSafetyGuard = OrderBundleSafetyGuard(config.orderBundleSafetyGuard, tickers, extendedTickers, dataAge)

  val logScheduleRate: FiniteDuration = FiniteDuration(config.statsInterval.toNanos, TimeUnit.NANOSECONDS)
  val logSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(30.seconds, logScheduleRate, self, LogStats())

  val deathWatchSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(3.minutes, 1.minute, self, DeathWatch())

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
    log.info(s"${Emoji.Robot}  Available liquidity sums: $liquidityPerExchange")

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


  def dropTradePairSync(exchangeName: String, tp: TradePair): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    Await.result(exchanges(exchangeName) ? RemoveTradePair(tp), timeout.duration)
  }

  /**
   * Select none-reserve assets, where not a single compatible Tradepair can be found looking at all exchanges.
   * Then we drop all TradePairs, connected to the selected ones.
   *
   * For instance we have an asset X (which is not one of the reserve assets), and the only tradable options are:
   * X:BTC on exchange1 and X:ETH on exchange2. Thus there is no compatible tradepair, we remove both trade pairs.
   *
   * This method runs non-parallel and synchronously to finish together with all actions finished
   * Parallel optimization is possible but not necessary for this small task
   */
  def dropUnusableTradepairsSync(): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    var eTradePairs: Set[Tuple2[String, TradePair]] = Set()
    for (exchange: String <- exchanges.keys) {
      val tp: Set[TradePair] = Await.result((exchanges(exchange) ? GetTradePairs()).mapTo[Set[TradePair]], timeout.duration)
      eTradePairs = eTradePairs ++ tp.map(e => (exchange, e))
    }
    val assetsToRemove = eTradePairs
      .map(_._2.baseAsset) // set of candidate assets
      .filterNot(Config.liquidityManager.reserveAssets.contains) // don't select reserve assets
      .filterNot(a =>
        eTradePairs
          .filter(_._2.baseAsset == a) // all connected tradepairs X -> ...
          .groupBy(_._2.quoteAsset) // grouped by other side of TradePair
          .values.exists(_.size > 1)) // tests, if a single TradePair exists (for our candidate base asset), that is present on at least two exchanges

    for (asset <- assetsToRemove) {
      val tradePairsToDrop: Set[Tuple2[String, TradePair]] =
        eTradePairs.filter(_._2.baseAsset == asset)

      log.info(s"${Emoji.Robot}  Dropping all TradePairs connected to $asset, because there are no compatible TradePair on any exchange:  $tradePairsToDrop")
      tradePairsToDrop.foreach(e => dropTradePairSync(e._1, e._2))
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
        Config.exchange(exchangeName),
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
    for (name: String <- Config.activeExchanges) {
      runExchange(name, GlobalConfig.AllExchanges(name))
    }
  }

  def startTraders(): Unit = {
    traders += "FooTrader" -> context.actorOf(
      FooTrader.props(Config.trader("foo-trader"), self, tradeContext),
      "FooTrader")
  }

  def cleanupTradePairs(): Unit = {
    dropUnusableTradepairsSync()
    log.info(s"${
      Emoji.Robot
    }  Finished cleanup of unusable trade pairs")
  }

  def startStreaming(): Unit = {
    for (exchange: ActorRef <- exchanges.values) {
      exchange ! StartStreaming()
    }
  }

  def initialized: Boolean = exchanges.keySet == initializedExchanges

  override def preStart(): Unit = {
    startExchanges()
    cleanupTradePairs()
    startStreaming()
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  /**
   * Watches for stale data and will trigger a restart of the TradeRoom
   */
  def deathWatch(): Unit = {
    dataAge.keys.foreach {
      e =>
        if (Duration.between(dataAge(e).tickerTS, Instant.now).compareTo(config.restartWhenAnExchangeDataStreamIsOlderThan) > 0) {
          log.info(s"${
            Emoji.Robot
          }  Killing TradeRoom actor because of outdated ticker data from $e")
          self ! Kill
        }
    }
  }

  def receive: Receive = {

    // messages from Exchanges
    case Exchange.Initialized(exchange) =>
      initializedExchanges = initializedExchanges + exchange
      if (initialized) {
        log.info(s"${
          Emoji.Satisfied
        }  All exchanges initialized")
        self ! LogStats()
        startTraders()
      }

    // messages from Traders

    case ob: OrderBundle =>
      placeOrderBundleOrders(ob)

    // messages from outself

    case LogStats() =>
      if (initialized)
        logStats()

    case DeathWatch() =>
      if (initialized)
        deathWatch()

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO shudown app in case of serious exceptions
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
