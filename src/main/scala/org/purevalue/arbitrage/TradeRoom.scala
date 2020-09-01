package org.purevalue.arbitrage

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.{GetTradePairs, RemoveTradePair, StartStreaming}
import org.purevalue.arbitrage.ExchangeAccountDataManager.{CancelOrder, CancelOrderResult, NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.ExchangeLiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.TradeRoom.{DeathWatch, LogStats, OrderBundle, OrderManagementSupervisor, OrderRef, OrderUpdateTrigger, TradeContext}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.purevalue.arbitrage.trader.FooTrader
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Success


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

  case class OrderRef(exchange: String, tradePair: TradePair, externalOrderId: String)
  case class OrderBundle(orderRequestBundle: OrderRequestBundle,
                         lockedLiquidity: Seq[LiquidityLock],
                         orders: Seq[OrderRef],
                         var finishTime: Option[Instant] = None)

  // communication with ourself
  case class OrderManagementSupervisor()
  case class LogStats()
  case class DeathWatch()
  // from ExchangeAccountDataManager
  case class OrderUpdateTrigger(exchange: String, externalOrderId: String) // status of an order has changed
  case class BalanceUpdateTrigger(exchange: String)

  /**
   * find the best ticker stats for the tradepair prioritized by exchange via config
   */
  def findReferenceTicker(tradePair: TradePair, extendedTickers: scala.collection.Map[String, scala.collection.Map[TradePair, ExtendedTicker]]): Option[ExtendedTicker] = {
    for (exchange <- Config.tradeRoom.extendedTickerExchanges) {
      extendedTickers.get(exchange) match {
        case Some(eTickers) => eTickers.get(tradePair) match {
          case Some(ticker) => return Some(ticker)
          case None =>
        }
        case None =>
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
  private var wallets: Map[String, Wallet] = Map()
  private val dataAge: ConcurrentMap[String, TPDataTimestamps] = TrieMap()

  // Map(exchange-name -> Map(external-order-id -> order)) contains incoming order & order-update data from exchanges
  private val activeOrders: ConcurrentMap[String, ConcurrentMap[String, Order]] = TrieMap()
  private var activeOrderBundles: Map[UUID, OrderBundle] = Map() // Map(order-bundle-id -> OrderBundle) maintained by TradeRoom
  private var finishedOrderBundles: ListBuffer[OrderBundle] = ListBuffer()

  private val fees: Map[String, Fee] = Map( // TODO query from exchange
    "binance" -> Fee("binance", Config.exchange("binance").makerFee, Config.exchange("binance").takerFee),
    "bitfinex" -> Fee("bitfinex", Config.exchange("bitfinex").makerFee, Config.exchange("bitfinex").takerFee)
  )

  private val tradeContext: TradeContext = TradeContext(tickers, extendedTickers, orderBooks, wallets, fees)
  private var firstOrdersAlreadyPlaced: Boolean = false

  private val orderBundleSafetyGuard = OrderBundleSafetyGuard(config.orderBundleSafetyGuard, tickers, extendedTickers, dataAge)

  val orderManagementSupervisorSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, 1.second, self, OrderManagementSupervisor())
  val logScheduleRate: FiniteDuration = FiniteDuration(config.stats.reportInterval.toNanos, TimeUnit.NANOSECONDS)
  val logSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(30.seconds, logScheduleRate, self, LogStats())
  val deathWatchSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(3.minutes, 1.minute, self, DeathWatch())

  /**
   * Lock liquidity
   *
   * @return all (locked=true) or nothing
   */
  def lockRequiredLiquidity(coins: Seq[LocalCryptoValue], dontUseTheseReserveAssets: Set[Asset]): Future[List[LiquidityLock]] = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeout
    Future.sequence(
      coins
        .groupBy(_.exchange)
        .map {
          x => (exchanges(x._1) ? LiquidityRequest(UUID.randomUUID(), Instant.now(), x._1, x._2, dontUseTheseReserveAssets)).mapTo[Option[LiquidityLock]]
        }) map {
      case x if x.forall(_.isDefined) =>
        x.flatten.toList
      case x => // release all other locks in case of partial non-success
        x.filter(_.isDefined).flatten.foreach { e =>
          exchanges(e.exchange) ! LiquidityLockClearance(e.liquidityRequestId)
        }
        List()
    } recover {
      case e: Exception =>
        log.error("Error while locking liquidity", e)
        List()
    }
  }

  def placeOrders(orderRequests: List[OrderRequest]): List[OrderRef] = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeout
    val orders: List[NewOrderAck] = Await.result(
      (context.actorOf(OrderSetPlacer.props(exchanges)) ? orderRequests.map(o => NewLimitOrder(o))).mapTo[List[NewOrderAck]],
      timeout.duration.plus(500.millis))
    orders.map(e => OrderRef(e.exchange, e.tradePair, e.externalOrderId))
  }

  def registerOrderBundle(b: OrderRequestBundle, lockedLiquidity: Seq[LiquidityLock], orders: Seq[OrderRef]): Unit = {
    activeOrderBundles = activeOrderBundles + (b.id -> OrderBundle(b, lockedLiquidity, orders))
  }

  def tryToPlaceOrderBundle(bundle: OrderRequestBundle): Unit = {
    if (!config.productionMode && firstOrdersAlreadyPlaced) {
      log.debug("Ignoring further order request bundle, because we are NOT in production mode")
      return
    }

    val isSafe: (Boolean, Option[Double]) = orderBundleSafetyGuard.isSafe(bundle, activeOrderBundles)
    if (isSafe._1) {
      val totalWin: Double = isSafe._2.get
      val requiredLiquidity: Seq[LocalCryptoValue] = bundle.orders.map(_.calcOutgoingLiquidity)

      lockRequiredLiquidity(requiredLiquidity, bundle.involvedReserveAssets) onComplete {

        case Success(lockedLiquidity: List[LiquidityLock]) if lockedLiquidity.nonEmpty =>
          val orderRefs: List[OrderRef] = placeOrders(bundle.orders)
          registerOrderBundle(bundle, lockedLiquidity, orderRefs)
          firstOrdersAlreadyPlaced = true
          log.info(s"${Emoji.Excited}  Placed checked $bundle (estimated total win: ${formatDecimal(totalWin, 2)})")
        // TODO don't forget to unlock requested liquidity after order is completed

        case Success(x) if x.isEmpty =>
          log.info(s"${Emoji.Robot}  Liquidity for trades not yet available: $requiredLiquidity")
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

    val liquiditySumCurrency: Asset = Config.tradeRoom.stats.aggregatedliquidityReportAsset
    val inconvertibleAssets = wallets
      .flatMap(_._2.balances.keys)
      .filter(e => CryptoValue(e, 1.0).convertTo(liquiditySumCurrency, tradeContext).isEmpty)
      .toSet
    if (inconvertibleAssets.nonEmpty) {
      log.warn(s"Currently we cannot calculate the correct balance, because no reference ticker available for converting them to $liquiditySumCurrency: $inconvertibleAssets")
    }
    val liquidityPerExchange: String =
      wallets.map { case (exchange, b) => (
        exchange,
        CryptoValue(
          liquiditySumCurrency,
          b.balances
            .filterNot(e => inconvertibleAssets.contains(e._1))
            .map(e => CryptoValue(e._2.asset, e._2.amountAvailable).convertTo(liquiditySumCurrency, tradeContext).get)
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

    log.info(s"${Emoji.Robot}  OrderBundleSafetyGuard decision stats: ${orderBundleSafetyGuard.unsafeStats}")
  }


  def dropTradePairSync(exchangeName: String, tp: TradePair): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    Await.result(exchanges(exchangeName) ? RemoveTradePair(tp), timeout.duration.plus(500.millis))
  }

  /**
   * Select none-reserve assets, where not at least two connected compatible TradePairs can be found looking at all exchanges.
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
   */
  def dropUnusableTradepairsSync(): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    var eTradePairs: Set[Tuple2[String, TradePair]] = Set()
    for (exchange: String <- exchanges.keys) {
      val tp: Set[TradePair] = Await.result((exchanges(exchange) ? GetTradePairs()).mapTo[Set[TradePair]], timeout.duration.plus(500.millis))
      eTradePairs = eTradePairs ++ tp.map(e => (exchange, e))
    }
    val assetsToRemove = eTradePairs
      .map(_._2.baseAsset) // set of candidate assets
      .filterNot(Config.liquidityManager.reserveAssets.contains) // don't select reserve assets
      .filterNot(a =>
        eTradePairs
          .filter(_._2.baseAsset == a) // all connected tradepairs X -> ...
          .groupBy(_._2.quoteAsset) // grouped by other side of TradePair (trade options)
          .count(e => e._2.size > 1) > 1 // tests, if at least two trade options exists (for our candidate base asset), that are present on at least two exchanges
      )

    for (asset <- assetsToRemove) {
      val tradePairsToDrop: Set[Tuple2[String, TradePair]] =
        eTradePairs.filter(_._2.baseAsset == asset)

      log.info(s"${Emoji.Robot}  Dropping all TradePairs involving $asset, because there are not enough (> 1) compatible TradePairs on any exchange:  $tradePairsToDrop")
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
    activeOrders += exchangeName -> TrieMap()

    exchanges += exchangeName -> context.actorOf(
      Exchange.props(
        exchangeName,
        Config.exchange(exchangeName),
        self,
        exchangeInit,
        ExchangeTPData(
          tickers(exchangeName),
          extendedTickers(exchangeName),
          orderBooks(exchangeName),
          dataAge(exchangeName)
        ),
        IncomingExchangeAccountData(
          wallets(exchangeName),
          activeOrders(exchangeName)
        )
      ), camelName)
  }

  def startExchanges(): Unit = {
    for (name: String <- Config.activeExchanges) {
      runExchange(name, StaticConfig.AllExchanges(name))
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

  /**
   * Will trigger a restart of the TradeRoom if stale data is found
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

  def shutdownAfterFirstOrderInNonProductionMode(): Unit = {
    if (!config.productionMode && firstOrdersAlreadyPlaced) {
      activeOrders.nonEmpty
    }
  }


  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 90.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  override def preStart(): Unit = {
    startExchanges()
    // REMARK: doing the tradepair cleanup here causes loss of some options for the reference ticker which leads to lesser balance conversion calculation options
    // TODO decouple reference-ticker delivery from ExchangeTPDataManager (=active trade pairs)
    cleanupTradePairs()
    startStreaming()
  }

  def activeOrder(ref: OrderRef): Option[Order] = {
    activeOrders(ref.exchange).get(ref.externalOrderId) match {
      case o: Some[Order] => o
      case _ =>
        log.warn(s"active order for $ref not found")
        None
    }
  }

  def clearLockedLiquidity(lockedLiquidity: Seq[LiquidityLock]): Unit = {
    lockedLiquidity.foreach { e =>
      exchanges(e.exchange) ! LiquidityLockClearance(e.liquidityRequestId)
    }
  }

  // cleanup completed "active" order bundle
  def cleanupOrderBundle(orderBundleId: UUID): Unit = {
    val orderBundle = activeOrderBundles(orderBundleId)
    orderBundle.finishTime = Some(Instant.now)
    finishedOrderBundles.append(orderBundle)
    activeOrderBundles = activeOrderBundles - orderBundleId
    clearLockedLiquidity(orderBundle.lockedLiquidity)
  }

  def checkActiveOrderBundleForCompletion(orderBundle: OrderBundle): Unit = {
    val orderBundleId: UUID = orderBundle.orderRequestBundle.id
    val orders: Seq[Order] = orderBundle.orders.flatMap(ref => activeOrder(ref))
    orders match {
      case o: Seq[Order] if o.isEmpty =>
        log.error(s"No order present for $orderBundle -> finishing it")
        cleanupOrderBundle(orderBundleId)
      case o: Seq[Order] if o.forall(_.orderStatus == OrderStatus.FILLED) =>
        log.debug(s"All orders of $orderBundle FILLED -> finishing it")
        cleanupOrderBundle(orderBundleId)
        log.info(s"OrderBundle $orderBundleId successfully finished")
      case o: Seq[Order] if o.forall(_.orderStatus.isFinal) =>
        log.debug(s"All orders of $orderBundle have a final state -> finishing it")
        cleanupOrderBundle(orderBundleId)
        log.warn(s"Finished OrderBundle $orderBundleId, but NOT all order are FILLED: $orders")
      case o: Seq[Order] => // order bundle still active
    }
  }

  def onOrderUpdate(exchange: String, externalOrderId: String): Unit = {
    val orderBundle: Option[OrderBundle] = activeOrderBundles.values.find(e =>
      e.orders.exists(o => o.exchange == exchange && o.externalOrderId == externalOrderId))

    if (orderBundle.isEmpty) {
      log.error(s"Got order-update but cannot find active order bundle with OrderUpdate")
    } else {
      checkActiveOrderBundleForCompletion(orderBundle.get)
    }
  }

  def receive: Receive = {

    // messages from Exchanges
    case Exchange.Initialized(exchange) =>
      initializedExchanges = initializedExchanges + exchange
      if (initialized) {
        log.info(s"${Emoji.Satisfied}  All exchanges initialized")
        self ! LogStats()
        startTraders()
      }

    // messages from Traders
    case ob: OrderRequestBundle => tryToPlaceOrderBundle(ob)

    // messages from outself
    case OrderManagementSupervisor() =>
      shutdownAfterFirstOrderInNonProductionMode()
    // TODO handleNotWorkingAgedOrderBundles()

    case OrderUpdateTrigger(exchange, externalOrderId) =>
      onOrderUpdate(exchange, externalOrderId)

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
