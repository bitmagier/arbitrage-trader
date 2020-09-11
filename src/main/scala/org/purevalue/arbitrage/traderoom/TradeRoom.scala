package org.purevalue.arbitrage.traderoom

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, OneForOneStrategy, PoisonPill, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.FooTrader
import org.purevalue.arbitrage.traderoom.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.traderoom.Exchange.{GetTradePairs, RemoveTradePair, StartStreaming}
import org.purevalue.arbitrage.traderoom.ExchangeAccountDataManager.{NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.traderoom.ExchangeLiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.OrderSetPlacer.NewOrderSet
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


object TradeRoom {

  type TickersReadonly = collection.Map[String, collection.Map[TradePair, Ticker]]
  type ActiveOrderBundlesReadonly = collection.Map[UUID, OrderBundle]

  /**
   * An always-uptodate view on the TradeRoom Pre-Trade Data.
   * Modification of the content is NOT permitted by users of the TRadeContext (even if technically possible)!
   */
  case class TradeContext(tickers: TickersReadonly,
                          orderBooks: collection.Map[String, collection.Map[TradePair, OrderBook]],
                          balances: collection.Map[String, Wallet],
                          fees: collection.Map[String, Fee],
                          doNotTouch: Map[String, Seq[Asset]]) {
    def referenceTicker: collection.Map[TradePair, Ticker] = tickers(Config.tradeRoom.referenceTickerExchange)
  }

  case class OrderRef(exchange: String, tradePair: TradePair, externalOrderId: String)

  case class OrderBundle(orderRequestBundle: OrderRequestBundle,
                         lockedLiquidity: Seq[LiquidityLock],
                         ordersRefs: Seq[OrderRef])
  case class FinishedOrderBundle(bundle: OrderBundle,
                                 finishedOrders: Seq[Order],
                                 finishTime: Instant,
                                 bill: OrderBill) {
    def shortDesc: String = s"FinishedOrderBundle(${finishedOrders.map(o => o.shortDesc).mkString(" & ")})"

  } // with sumUSDT at finish time time
  case class LiquidityTx(orderRequest: OrderRequest,
                         orderRef: OrderRef,
                         creationTime: Instant)
  case class FinishedLiquidityTx(liquidityTx: LiquidityTx,
                                 finishedOrder: Order,
                                 finishTime: Instant,
                                 bill: OrderBill)

  // communication with ourself
  case class OrderManagementSupervisor()
  case class LogStats()
  case class DeathWatch()
  // from ExchangeAccountDataManager
  case class OrderUpdateTrigger(ref: OrderRef) // status of an order has changed
  case class WalletUpdateTrigger(exchange: String)
  // from liquidity managers
  case class LiquidityTransformationOrder(orderRequest: OrderRequest)

  def props(config: TradeRoomConfig, exchangesConfig: Map[String, ExchangeConfig], liquidityManagerConfig: LiquidityManagerConfig): Props =
    Props(new TradeRoom(config, exchangesConfig, liquidityManagerConfig))
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom(config: TradeRoomConfig,
                exchangesConfig: Map[String, ExchangeConfig],
                liquidityManagerConfig: LiquidityManagerConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private var exchanges: Map[String, ActorRef] = Map()
  private var initializedExchanges: Set[String] = Set()
  private var traders: Map[String, ActorRef] = Map()

  // a map per exchange
  type ConcurrentMap[A, B] = collection.concurrent.Map[A, B]
  private val tickers: ConcurrentMap[String, ConcurrentMap[TradePair, Ticker]] = TrieMap()
  private val orderBooks: ConcurrentMap[String, ConcurrentMap[TradePair, OrderBook]] = TrieMap()
  private var wallets: Map[String, Wallet] = Map()
  private val dataAge: ConcurrentMap[String, TPDataTimestamps] = TrieMap()

  private def referenceTicker: collection.Map[TradePair, Ticker] = tickers(config.referenceTickerExchange)

  // Map(exchange-name -> Map(order-ref -> order)) contains incoming order & order-update data from exchanges data stream
  private val activeOrders: ConcurrentMap[String, ConcurrentMap[OrderRef, Order]] = TrieMap()

  // housekeeping of our requests: OrderBundles + LiquidityTx
  private val openOrderBundles: ConcurrentMap[UUID, OrderBundle] = TrieMap() // Map(order-bundle-id -> OrderBundle) maintained by TradeRoom
  private val openLiquidityTx: ConcurrentMap[OrderRef, LiquidityTx] = TrieMap() // maintained by TradeRoom
  private var finishedOrderBundles: List[FinishedOrderBundle] = List()
  private var finishedLiquidityTxs: List[FinishedLiquidityTx] = List()

  private val fees: Map[String, Fee] =
    exchangesConfig.values.map(e => (e.exchangeName, e.fee)).toMap // TODO query from exchange

  private val doNotTouchAssets: Map[String, Seq[Asset]] =
    exchangesConfig.values.map(e => (e.exchangeName, e.doNotTouchTheseAssets)).toMap

  private val tradeContext: TradeContext = TradeContext(tickers, orderBooks, wallets, fees, doNotTouchAssets)

  private val orderBundleSafetyGuard = new OrderBundleSafetyGuard(config.orderBundleSafetyGuard, exchangesConfig, tradeContext, dataAge, openOrderBundles)

  val orderManagementSupervisorSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, 1.second, self, OrderManagementSupervisor())
  val logScheduleRate: FiniteDuration = FiniteDuration(config.stats.reportInterval.toNanos, TimeUnit.NANOSECONDS)
  val logSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(30.seconds, logScheduleRate, self, LogStats())
  val deathWatchSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(3.minutes, 1.minute, self, DeathWatch())

  /**
   * Lock liquidity
   *
   * @return all (locked=true) or nothing
   */
  def lockRequiredLiquidity(tradePattern: String, coins: Seq[LocalCryptoValue], dontUseTheseReserveAssets: Set[Asset]): Future[List[LiquidityLock]] = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeout
    Future.sequence(
      coins
        .groupBy(_.exchange)
        .map { x =>
          (exchanges(x._1) ?
            LiquidityRequest(
              UUID.randomUUID(),
              Instant.now(),
              x._1,
              tradePattern,
              x._2.map(c => CryptoValue(c.asset, c.amount)),
              dontUseTheseReserveAssets))
            .mapTo[Option[LiquidityLock]]
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

  def placeOrders(orderRequests: List[OrderRequest]): Future[List[OrderRef]] = {
    implicit val timeout: Timeout = Config.httpTimeout.mul(2) // covers parallel order request + possible order cancel operations

    (context.actorOf(OrderSetPlacer.props(exchanges)) ? NewOrderSet(orderRequests.map(o => NewLimitOrder(o))))
      .mapTo[List[NewOrderAck]]
      .map(_.map(_.toOrderRef))
  }

  def placeLiquidityTransformationOrder(request: OrderRequest): Unit = {
    if (doNotTouchAssets(request.exchange).intersect(request.tradePair.involvedAssets).nonEmpty) throw new IllegalArgumentException

    // this should not occur - but here is a last guard
    if (openLiquidityTx.keys.exists(ref => ref.exchange == request.exchange && ref.tradePair == request.tradePair)) {
      log.warn(s"Ignoring liquidity tx because a similar one (same trade pair on same exchange) is still in place: $request")
      return
    }

    if (config.oneTradeOnlyTestMode && (finishedLiquidityTxs.nonEmpty || openLiquidityTx.nonEmpty)) {
      log.debug("Ignoring further liquidity tx, because we are NOT in production mode")
      return
    }

    implicit val timeout: Timeout = Config.httpTimeout.plus(Config.internalCommunicationTimeout.duration)
    (exchanges(request.exchange) ? NewLimitOrder(request)).mapTo[NewOrderAck].onComplete {
      case Success(ack) =>
        if (log.isTraceEnabled) log.trace(s"successfully placed liquidity tx order $ack")
        val ref = ack.toOrderRef
        openLiquidityTx.update(ref, LiquidityTx(request, ref, Instant.now))
      case Failure(e) =>
        log.error("placing liquidity order failed", e)
    }
  }

  def registerOrderBundle(b: OrderRequestBundle, lockedLiquidity: Seq[LiquidityLock], orders: Seq[OrderRef]) {
    openOrderBundles.update(b.id, OrderBundle(b, lockedLiquidity, orders))
  }

  def tryToPlaceOrderBundle(bundle: OrderRequestBundle): Unit = {
    if (bundle.orderRequests.exists(e =>
      doNotTouchAssets(e.exchange).intersect(e.tradePair.involvedAssets).nonEmpty)) {
      log.debug(s"ignoring $bundle containing a DO-NOT-TOUCH asset")
    }
    if (config.oneTradeOnlyTestMode && (finishedOrderBundles.nonEmpty || openOrderBundles.nonEmpty)) {
      log.debug("Ignoring further order request bundle, because we are NOT in production mode")
      return
    }

    val isSafe: (Boolean, Option[Double]) = orderBundleSafetyGuard.isSafe(bundle)
    if (isSafe._1) {
      val totalWin: Double = isSafe._2.get
      val requiredLiquidity: Seq[LocalCryptoValue] = bundle.orderRequests.map(_.calcOutgoingLiquidity)

      lockRequiredLiquidity(bundle.tradePattern, requiredLiquidity, bundle.involvedReserveAssets) onComplete {

        case Success(lockedLiquidity: List[LiquidityLock]) if lockedLiquidity.nonEmpty =>

          placeOrders(bundle.orderRequests) onComplete {

            case Success(orderRefs: List[OrderRef]) =>
              registerOrderBundle(bundle, lockedLiquidity, orderRefs)
              log.info(s"${Emoji.Excited}  Placed checked $bundle (estimated total win: ${formatDecimal(totalWin, 2)})")

            case Failure(e) =>
              log.error("placing orders failed", e)
          }

        case Success(x) if x.isEmpty =>
          log.info(s"${Emoji.Robot}  Liquidity for trades not yet available: $requiredLiquidity")
      }
    }
  }

  def dropTradePairSync(exchangeName: String, tp: TradePair): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    Await.result(exchanges(exchangeName) ? RemoveTradePair(tp), timeout.duration.plus(500.millis))
  }


  def queryTradePairs(exchange: String): Set[TradePair] = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    Await.result((exchanges(exchange) ? GetTradePairs()).mapTo[Set[TradePair]], timeout.duration.plus(500.millis))
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
   *
   * Note:
   * For liquidity conversion and some calculations we need USDT pairs in ReferenceTicker, so for now we don't drop x:USDT pairs (until ReferenceTicker is decoupled from exchange TradePairs)
   * Also - if no x:USDT pair is available, we don't drop the x:BTC pair (like for IOTA on Bitfinex we only have IOTA:BTC & IOTA:ETH)
   */
  def dropUnusableTradepairsSync(): Unit = {
    var eTradePairs: Set[Tuple2[String, TradePair]] = Set()
    for (exchange: String <- exchanges.keys) {
      val tp: Set[TradePair] = queryTradePairs(exchange)
      eTradePairs = eTradePairs ++ tp.map(e => (exchange, e))
    }
    val assetsToRemove: Set[Asset] = eTradePairs
      .map(_._2.baseAsset) // set of candidate assets
      .filterNot(e => exchangesConfig.values.exists(_.reserveAssets.contains(e))) // don't select reserve assets
      .filterNot(a =>
        eTradePairs
          .filter(_._2.baseAsset == a) // all connected tradepairs X -> ...
          .groupBy(_._2.quoteAsset) // grouped by other side of TradePair (trade options)
          .count(e => e._2.size > 1) > 1 // tests, if at least two trade options exists (for our candidate base asset), that are present on at least two exchanges
      )

    for (asset <- assetsToRemove) {
      val tradePairsToDrop: Set[Tuple2[String, TradePair]] =
        eTradePairs
          .filter(e => e._2.baseAsset == asset && e._2.quoteAsset != USDT) // keep :USDT TradePairs because we want them in the ReferenceTicker
          .filterNot(e => !eTradePairs.exists(x => x._1 == e._1 && x._2 == TradePair(e._2.baseAsset, USDT)) && // when no :USDT tradepair exists
            e._2 == TradePair(e._2.baseAsset, Bitcoin)) // keep :BTC tradepair (for currency conversion via x -> BTC -> USDT)

      if (tradePairsToDrop.nonEmpty) {
        log.debug(s"${Emoji.Robot}  Dropping some TradePairs involving $asset, because we don't have a use for it:  $tradePairsToDrop")
        tradePairsToDrop.foreach(e => dropTradePairSync(e._1, e._2))
      }
    }
  }


  def runExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    val camelName = exchangeName.substring(0, 1).toUpperCase + exchangeName.substring(1)
    tickers += exchangeName -> TrieMap[TradePair, Ticker]()
    orderBooks += exchangeName -> TrieMap[TradePair, OrderBook]()
    dataAge += exchangeName -> TPDataTimestamps(None, Instant.MIN, None)

    wallets += exchangeName -> Wallet(exchangeName, Map(), exchangesConfig(exchangeName))
    activeOrders += exchangeName -> TrieMap()

    exchanges = exchanges + (exchangeName -> context.actorOf(
      Exchange.props(
        exchangeName,
        exchangesConfig(exchangeName),
        liquidityManagerConfig,
        self,
        exchangeInit,
        ExchangeTPData(
          tickers(exchangeName),
          orderBooks(exchangeName),
          dataAge(exchangeName)
        ),
        IncomingExchangeAccountData(
          wallets(exchangeName),
          activeOrders(exchangeName)
        ),
        () => referenceTicker,
        () => openLiquidityTx.filter(_._1.exchange == exchangeName).values
      ), camelName))
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
    log.info(s"${Emoji.Robot}  Finished cleanup of unusable trade pairs")
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
      e => {
        val lastSeen: Instant = (dataAge(e).heartbeatTS.toSeq ++ dataAge(e).orderBookTS.toSeq ++ Seq(dataAge(e).tickerTS)).max
        if (Duration.between(lastSeen, Instant.now).compareTo(config.restartWhenAnExchangeDataStreamIsOlderThan) > 0) {
          log.info(s"${
            Emoji.Robot
          }  Killing TradeRoom actor because of outdated ticker data from $e")
          self ! Kill
        }
      }
    }
  }

  def shutdownAfterFirstOrderInNonProductionMode(): Unit = {
    if (config.oneTradeOnlyTestMode && finishedOrderBundles.nonEmpty) {
      log.info(
        s"""${Emoji.Robot}  Shutting down TradeRoom after first finished order bundle, as configured  (trade-room.production-mode)
           |activeOrders: $activeOrders
           |openOrderBundles: $openOrderBundles
           |openLiquidityTx: $openLiquidityTx
           |finishedOrderBundles: $finishedOrderBundles
           |finishedLiquidityTx: $finishedLiquidityTxs""".mkString("\n"))
      self ! PoisonPill
    }
  }

  def logStats(): Unit = {
    def toEntriesPerExchange[T](m: collection.Map[String, collection.Map[TradePair, T]]): String = {
      m.map(e => (e._1, e._2.values.size))
        .toSeq
        .sortBy(_._1)
        .map(e => s"${e._1}:${e._2}")
        .mkString(", ")
    }

    for (w <- wallets.values) {
      val walletOverview: String = w.toOverviewString(config.stats.aggregatedliquidityReportAsset, tickers(w.exchange))
      log.info(s"${Emoji.Robot}  $walletOverview")
    }

    val freshestTicker = dataAge.maxBy(_._2.tickerTS.toEpochMilli)
    val oldestTicker = dataAge.minBy(_._2.tickerTS.toEpochMilli)
    log.info(s"${Emoji.Robot}  TradeRoom stats: [general] " +
      s"ticker:[${toEntriesPerExchange(tickers)}]" +
      s" (oldest: ${oldestTicker._1} ${Duration.between(oldestTicker._2.tickerTS, Instant.now).toMillis} ms," +
      s" freshest: ${freshestTicker._1} ${Duration.between(freshestTicker._2.tickerTS, Instant.now).toMillis} ms)" +
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

    log.info(s"${Emoji.Robot}  OrderBundleSafetyGuard decision stats: [${orderBundleSafetyGuard.unsafeStats.mkString("|")}]")

    val now = Instant.now

    val lastHourArbitrageSumUSDT: Double = finishedOrderBundles
      .filter(b => Duration.between(b.finishTime, now).toHours < 1)
      .map(_.bill.sumUSDT)
      .foldLeft(0.0)((sum, x) => sum + x)
    val lastHourLiquidityTxSumUSDT: Double = finishedLiquidityTxs
      .filter(b => Duration.between(b.finishTime, now).toHours < 1)
      .map(_.bill.sumUSDT)
      .foldLeft(0.0)((sum, x) => sum + x)
    val lastHourSumUSDT: Double = lastHourArbitrageSumUSDT + lastHourLiquidityTxSumUSDT
    log.info(s"${Emoji.Robot}  Last 1h: cumulated gain: ${formatDecimal(lastHourSumUSDT,2)} USDT " +
      s"(arbitrage orders: ${formatDecimal(lastHourArbitrageSumUSDT,2)} USDT, " +
      s"liquidity tx: ${formatDecimal(lastHourLiquidityTxSumUSDT)} USDT) ")

    val totalArbitrageSumUSDT: Double = finishedOrderBundles
      .map(_.bill.sumUSDT)
      .foldLeft(0.0)((sum, x) => sum + x)
    val totalLiquidityTxSumUSDT: Double = finishedLiquidityTxs
      .map(_.bill.sumUSDT)
      .foldLeft(0.0)((sum, x) => sum + x)
    val totalSumUSDT: Double = totalArbitrageSumUSDT + totalLiquidityTxSumUSDT
    log.info(s"${Emoji.Robot}  Total cumulated gain: ${formatDecimal(totalSumUSDT, 2)} USDT " +
      s"(arbitrage orders: ${formatDecimal(totalArbitrageSumUSDT,2)} USDT, liquidity tx: " +
      s"${formatDecimal(totalLiquidityTxSumUSDT,2)} USDT) ")

  }


  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 90.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  override def preStart(): Unit = {
    if (config.oneTradeOnlyTestMode || config.tradeSimulation) {
      log.info(s"Starting with oneTradeOnlyTestMode=${config.oneTradeOnlyTestMode} and tradeSimulation=${config.tradeSimulation}")
    } else {
      log.info(s"${Emoji.DoYouEvenLiftBro}  Starting in production mode")
    }

    startExchanges()
    // REMARK: doing the tradepair cleanup here causes loss of some options for the reference ticker which leads to lesser balance conversion calculation options
    cleanupTradePairs()
    startStreaming()
  }

  def activeOrder(ref: OrderRef): Option[Order] = {
    activeOrders(ref.exchange).get(ref) match {
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

  // cleanup completed order bundle, which is still in the "active" list
  def cleanupOrderBundle(orderBundleId: UUID): Unit = {
    val bundle: OrderBundle = openOrderBundles(orderBundleId)
    val orders: Seq[Order] = bundle.ordersRefs.flatMap(activeOrder)
    val finishTime = orders.map(_.lastUpdateTime).max

    val bill: OrderBill = OrderBill.calc(orders, tickers, exchangesConfig.map(e => (e._1, e._2.fee)))
    val finishedOrderBundle = FinishedOrderBundle(bundle, orders, finishTime, bill)
    this.synchronized {
      finishedOrderBundles = finishedOrderBundle :: finishedOrderBundles
    }
    openOrderBundles.remove(orderBundleId)
    bundle.ordersRefs.foreach {
      e => activeOrders(e.exchange).remove(e)
    }

    clearLockedLiquidity(bundle.lockedLiquidity)

    if (bill.sumUSDT >= 0) {
      val emoji = if (bill.sumUSDT >= 1.0) Emoji.Opera else Emoji.Winning
      log.info(s"$emoji  ${finishedOrderBundle.shortDesc} completed with a win of ${bill.sumUSDT}")
    } else {
      log.warn(s"${Emoji.SadFace}  ${finishedOrderBundle.shortDesc} completed with a loss of ${bill.sumUSDT} ${Emoji.LookingDown}: $finishedOrderBundle")
    }
  }


  def cleanupLiquidityTxOrder(tx: LiquidityTx): Unit = {
    val order: Order = activeOrder(tx.orderRef).get // must exist
    val bill: OrderBill = OrderBill.calc(Seq(order), tickers, exchangesConfig.map(e => (e._1, e._2.fee)))
    val finishedLiquidityTx = FinishedLiquidityTx(tx, order, order.lastUpdateTime, bill)
    this.synchronized {
      finishedLiquidityTxs = finishedLiquidityTx :: finishedLiquidityTxs
    }
    openLiquidityTx.remove(tx.orderRef)
    activeOrders(tx.orderRef.exchange).remove(tx.orderRef)
  }

  def cleanupFinishedOrderBundle(orderBundle: OrderBundle): Unit = {
    val orderBundleId: UUID = orderBundle.orderRequestBundle.id
    val orders: Seq[Order] = orderBundle.ordersRefs.flatMap(ref => activeOrder(ref))
    orders match {
      case order: Seq[Order] if order.isEmpty =>
        log.error(s"No order present for $orderBundle -> cleaning up")
        cleanupOrderBundle(orderBundleId)
      case order: Seq[Order] if order.forall(_.orderStatus == OrderStatus.FILLED) =>
        if (log.isTraceEnabled) log.trace(s"All orders of $orderBundle FILLED -> finishing it")
        cleanupOrderBundle(orderBundleId)
        log.info(s"${Emoji.Robot}  OrderBundle $orderBundleId successfully finished")
      case order: Seq[Order] if order.forall(_.orderStatus.isFinal) =>
        log.debug(s"${Emoji.Robot}  All orders of $orderBundle have a final state (${order.map(_.orderStatus).mkString(",")}) -> finished otherwise")
        cleanupOrderBundle(orderBundleId)
        log.warn(s"${Emoji.Robot}  Finished OrderBundle $orderBundleId, but NOT all order are FILLED: $orders")
      case order: Seq[Order] => // order bundle still active: nothing to do
        if (log.isTraceEnabled) log.trace(s"Watching minor order update for $orderBundle: $order")
    }
  }

  def cleanupPossiblyFinishedLiquidityTxOrder(tx: LiquidityTx): Unit = {
    activeOrder(tx.orderRef) match {
      case Some(order) if order.orderStatus == OrderStatus.FILLED =>
        log.info(s"${Emoji.Robot}  Liquidity tx ${tx.orderRef} FILLED")
        cleanupLiquidityTxOrder(tx)
      case Some(order) if order.orderStatus.isFinal =>
        log.info(s"${Emoji.Robot}  Liquidity tx ${tx.orderRef} has final state ${order.orderStatus} - ")
        cleanupLiquidityTxOrder(tx)
      case Some(order) => // order still active: nothing to do
        if (log.isTraceEnabled) log.trace(s"Watching liquidity tx minor order update: $order")

    }
  }

  def onOrderUpdate(ref: OrderRef): Unit = {
    openOrderBundles.values.find(e => e.ordersRefs.contains(ref)) match {
      case Some(orderBundle) =>
        cleanupFinishedOrderBundle(orderBundle)
        return
      case None => // proceed to next statement
    }

    openLiquidityTx.get(ref) match {
      case Some(liquidityTx) => cleanupPossiblyFinishedLiquidityTxOrder(liquidityTx)
      case None => log.error(s"Got order-update (${ref.exchange}: ${ref.externalOrderId}) but cannot find active order bundle or liquidity tx with that id")
    }
  }

  // TODO schedule job
  def houseKeeping(): Unit = {
    // TODO report entries in openOrders, which are not referenced by activeOrderBundle or activeLiquidityTx
    // TODO report apparently dead entries in openOrderBundle
    // TODO report apparently dead entries in openLiquidityTx
  }

  def receive: Receive = {

    // messages from Exchanges
    case Exchange.Initialized(exchange) =>
      initializedExchanges = initializedExchanges + exchange
      if (initialized) {
        log.info(s"${Emoji.Satisfied}  All exchanges initialized")
        startTraders()
      }

    // messages from Traders
    case bundle: OrderRequestBundle => tryToPlaceOrderBundle(bundle)

    // messages from ExchangeLiquidityManager
    case LiquidityTransformationOrder(orderRequest) => placeLiquidityTransformationOrder(orderRequest)

    // messages from outself
    case OrderManagementSupervisor() =>
      shutdownAfterFirstOrderInNonProductionMode()
    // TODO handleNotWorkingAgedOrderBundles()

    case OrderUpdateTrigger(orderRef) =>
      onOrderUpdate(orderRef)

    case t: WalletUpdateTrigger =>
      exchanges(t.exchange).forward(t)

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

// TODO single finished OrderBundle proof-of-concept check: If real win is a loss, we stop our application directly!
// TODO finished orderbundle statistics: Last hour: number of trades, Estimated Win, real win at tx times, real win now
// TODO decouple reference-ticker delivery from ExchangeTPDataManager (=active trade pairs), because Tradepair cleanup works against a rich reference ticker