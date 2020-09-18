package org.purevalue.arbitrage.traderoom

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, OneForOneStrategy, PoisonPill, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, CancelOrderResult, NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.adapter.{Fee, PublicDataTimestamps, Ticker, Wallet}
import org.purevalue.arbitrage.trader.FooTrader
import org.purevalue.arbitrage.traderoom.Asset.USDT
import org.purevalue.arbitrage.traderoom.OrderSetPlacer.NewOrderSet
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange.OrderUpdateTrigger
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object TradeRoom {
  type ConcurrentMap[A, B] = collection.concurrent.Map[A, B]
  type TickersReadonly = collection.Map[String, collection.Map[TradePair, Ticker]]
  type ActiveOrderBundlesReadonly = collection.Map[UUID, OrderBundle]

  /**
   * An always-uptodate view on the TradeRoom Pre-Trade Data.
   * Modification of the content is NOT permitted by users of the TRadeContext (even if technically possible)!
   */
  case class TradeContext(tickers: TickersReadonly,
                          referenceTickerExchange: String,
                          balances: collection.Map[String, Wallet],
                          fees: collection.Map[String, Fee],
                          doNotTouch: Map[String, Seq[Asset]]) {
    def referenceTicker: collection.Map[TradePair, Ticker] = tickers(referenceTickerExchange)
  }

  case class OrderRef(exchange: String, tradePair: TradePair, externalOrderId: String)

  case class OrderBundle(orderRequestBundle: OrderRequestBundle,
                         lockedLiquidity: Seq[LiquidityLock],
                         ordersRefs: Seq[OrderRef]) {
    def shortDesc: String = s"OrderBundle(${orderRequestBundle.id}, ${orderRequestBundle.tradeDesc})"
  }
  case class FinishedOrderBundle(bundle: OrderBundle,
                                 finishedOrders: Seq[Order],
                                 finishTime: Instant,
                                 bill: OrderBill) {
    def shortDesc: String = s"FinishedOrderBundle(${finishedOrders.map(o => o.shortDesc).mkString(" & ")})"
  }
  case class LiquidityTx(orderRequest: OrderRequest,
                         orderRef: OrderRef,
                         creationTime: Instant)
  case class FinishedLiquidityTx(liquidityTx: LiquidityTx,
                                 finishedOrder: Order,
                                 finishTime: Instant,
                                 bill: OrderBill)

  // communication with ourself
  case class LogStats()
  case class HouseKeeping()
  case class Stop(timeout: Duration)
  // from liquidity managers
  case class LiquidityTransformationOrder(orderRequest: OrderRequest)
  /**
   * Towards exchange.
   * Will be replied with a TradeRoomJoined() from exchange
   */
  case class JoinTradeRoom(tradeRoom: ActorRef,
                           findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
                           referenceTicker: () => collection.Map[TradePair, Ticker])
  case class TradeRoomJoined(exchange: String)

  def props(config: Config,
            exchanges: Map[String, ActorRef],
            tickers: Map[String, ConcurrentMap[TradePair, Ticker]],
            dataAge: Map[String, PublicDataTimestamps],
            wallets: Map[String, Wallet],
            activeOrders: Map[String, ConcurrentMap[OrderRef, Order]]): Props =
    Props(new TradeRoom(config, exchanges, tickers, dataAge, wallets, activeOrders))
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom(val config: Config,
                val exchanges: Map[String, ActorRef],
                val tickers: Map[String, ConcurrentMap[TradePair, Ticker]], // a map per exchange
                val dataAge: Map[String, PublicDataTimestamps],
                val wallets: Map[String, Wallet],
                val activeOrders: Map[String, ConcurrentMap[OrderRef, Order]] // Map(exchange-name -> Map(order-ref -> order)) contains incoming order & order-update data from exchanges data stream
               ) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  var traders: Map[String, ActorRef] = Map()

  private var shutdownInitiated: Boolean = false

  private def referenceTicker: collection.Map[TradePair, Ticker] = tickers(config.tradeRoom.referenceTickerExchange)

  // housekeeping of our requests: OrderBundles + LiquidityTx
  private val activeOrderBundles: ConcurrentMap[UUID, OrderBundle] = TrieMap() // Map(order-bundle-id -> OrderBundle) maintained by TradeRoom
  private val activeLiquidityTx: ConcurrentMap[OrderRef, LiquidityTx] = TrieMap() // maintained by TradeRoom
  private var finishedOrderBundles: List[FinishedOrderBundle] = List()
  private var finishedLiquidityTxs: List[FinishedLiquidityTx] = List()

  private val fees: Map[String, Fee] = config.tradeRoom.exchanges.values.map(e => (e.exchangeName, e.fee)).toMap // TODO query from exchange
  private val doNotTouchAssets: Map[String, Seq[Asset]] = config.tradeRoom.exchanges.values.map(e => (e.exchangeName, e.doNotTouchTheseAssets)).toMap
  private val tradeContext: TradeContext =
    TradeContext(
      tickers,
      config.tradeRoom.referenceTickerExchange,
      wallets,
      fees,
      doNotTouchAssets)

  private val orderBundleSafetyGuard = new OrderBundleSafetyGuard(config.tradeRoom.orderBundleSafetyGuard, config.tradeRoom.exchanges, tradeContext, dataAge, activeOrderBundles)

  val houseKeepingSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(5.seconds, 3.seconds, self, HouseKeeping())
  val logScheduleRate: FiniteDuration = FiniteDuration(config.tradeRoom.stats.reportInterval.toNanos, TimeUnit.NANOSECONDS)
  val logSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(3.minutes, logScheduleRate, self, LogStats())

  /**
   * Lock liquidity
   *
   * @return all (locked=true) or nothing
   */
  def lockRequiredLiquidity(tradePattern: String, coins: Seq[LocalCryptoValue], dontUseTheseReserveAssets: Set[Asset]): Future[List[LiquidityLock]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
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
    implicit val timeout: Timeout = config.global.httpTimeout.mul(2) // covers parallel order request + possible order cancel operations

    (context.actorOf(OrderSetPlacer.props(exchanges)) ? NewOrderSet(orderRequests.map(o => NewLimitOrder(o))))
      .mapTo[List[NewOrderAck]]
      .map(_.map(_.toOrderRef))
  }

  def placeLiquidityTransformationOrder(request: OrderRequest): Unit = {
    if (shutdownInitiated) return
    if (doNotTouchAssets(request.exchange).intersect(request.tradePair.involvedAssets).nonEmpty) throw new IllegalArgumentException

    // this should not occur - but here is a last guard
    if (activeLiquidityTx.keys.exists(ref => ref.exchange == request.exchange && ref.tradePair == request.tradePair)) {
      log.warn(s"Ignoring liquidity tx because a similar one (same trade pair on same exchange) is still in place: $request")
      return
    }

    implicit val timeout: Timeout = config.global.httpTimeout.plus(config.global.internalCommunicationTimeout.duration)
    (exchanges(request.exchange) ? NewLimitOrder(request)).mapTo[NewOrderAck].onComplete {
      case Success(ack) =>
        if (log.isTraceEnabled) log.trace(s"successfully placed liquidity tx order $ack")
        val ref = ack.toOrderRef
        activeLiquidityTx.update(ref, LiquidityTx(request, ref, Instant.now))
      case Failure(e) =>
        log.error("placing liquidity order failed", e)
    }
  }

  def registerOrderBundle(b: OrderRequestBundle, lockedLiquidity: Seq[LiquidityLock], orders: Seq[OrderRef]) {
    activeOrderBundles.update(b.id, OrderBundle(b, lockedLiquidity, orders))
  }

  def tryToPlaceOrderBundle(bundle: OrderRequestBundle): Unit = {
    if (shutdownInitiated) return

    if (bundle.orderRequests.exists(e =>
      doNotTouchAssets(e.exchange).intersect(e.tradePair.involvedAssets).nonEmpty)) {
      log.debug(s"ignoring $bundle containing a DO-NOT-TOUCH asset")
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

  def logStats(): Unit = {
    if (shutdownInitiated) return
    val now = Instant.now

    def logWalletOverview(): Unit = {
      for (w <- wallets.values) {
        val walletOverview: String = w.toOverviewString(config.tradeRoom.stats.aggregatedliquidityReportAsset, tickers(w.exchange))
        log.info(s"${Emoji.Robot}  $walletOverview")
      }
    }

    def toEntriesPerExchange[T](m: collection.Map[String, collection.Map[TradePair, T]]): String = {
      m.map(e => (e._1, e._2.values.size))
        .toSeq
        .sortBy(_._1)
        .map(e => s"${e._1}:${e._2}")
        .mkString(", ")
    }


    def logTickerStats(): Unit = {
      val freshestTicker = dataAge.maxBy(_._2.tickerTS.toEpochMilli)
      val oldestTicker = dataAge.minBy(_._2.tickerTS.toEpochMilli)
      log.info(s"${Emoji.Robot}  TradeRoom stats: [general] " +
        s"ticker:[${toEntriesPerExchange(tickers)}]" +
        s" (oldest: ${oldestTicker._1} ${Duration.between(oldestTicker._2.tickerTS, Instant.now).toMillis} ms," +
        s" freshest: ${freshestTicker._1} ${Duration.between(freshestTicker._2.tickerTS, Instant.now).toMillis} ms)")
    }


    def logOrderBundleSafetyGuardStats(): Unit = {
      log.info(s"${Emoji.Robot}  OrderBundleSafetyGuard decision stats: [${orderBundleSafetyGuard.unsafeStats.mkString("|")}]")
    }

    def logOrderGainStats(): Unit = {
      val lastHourArbitrageSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedOrderBundles
            .filter(b => Duration.between(b.finishTime, now).toHours < 1)
            .flatMap(_.bill.balanceSheet),
          USDT,
          tickers)
      val lastHourLiquidityTxSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedLiquidityTxs
            .filter(b => Duration.between(b.finishTime, now).toHours < 1)
            .flatMap(_.bill.balanceSheet),
          USDT,
          tickers)

      // TODO log a info message with aggregation on currency level
      val lastHourSumUSDT: Double = lastHourArbitrageSumUSDT + lastHourLiquidityTxSumUSDT
      log.info(s"${Emoji.Robot}  Last 1h: cumulated gain: ${formatDecimal(lastHourSumUSDT, 2)} USDT " +
        s"(arbitrage orders: ${formatDecimal(lastHourArbitrageSumUSDT, 2)} USDT, " +
        s"liquidity tx: ${formatDecimal(lastHourLiquidityTxSumUSDT, 2)} USDT) ")

      val totalArbitrageSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedOrderBundles
            .flatMap(_.bill.balanceSheet),
          USDT,
          tickers)
      val totalLiquidityTxSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedLiquidityTxs
            .flatMap(_.bill.balanceSheet),
          USDT,
          tickers)
      val totalSumUSDT: Double = totalArbitrageSumUSDT + totalLiquidityTxSumUSDT
      log.info(s"${Emoji.Robot}  Total cumulated gain: ${formatDecimal(totalSumUSDT, 2)} USDT " +
        s"(arbitrage orders: ${formatDecimal(totalArbitrageSumUSDT, 2)} USDT, liquidity tx: " +
        s"${formatDecimal(totalLiquidityTxSumUSDT, 2)} USDT) ")
    }

    def logFinalOrderStateStats(): Unit = {
      def orderStateStats(orders: Iterable[Order]): Map[OrderStatus, Int] =
        orders
          .groupBy(_.orderStatus)
          .map(e => (e._1, e._2.size))

      val liquidityTxOrders1h = finishedLiquidityTxs.map(_.finishedOrder)
      val orderBundleOrders1h = finishedOrderBundles.flatMap(_.finishedOrders)

      log.info(s"""${Emoji.Robot}  Last 1h final order status: trader tx:[${orderStateStats(orderBundleOrders1h).mkString(",")}], liquidity tx: [${orderStateStats(liquidityTxOrders1h).mkString(",")}]""")
    }

    logWalletOverview()
    logTickerStats()
    logOrderBundleSafetyGuardStats()
    logFinalOrderStateStats()
    logOrderGainStats()
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
    val bundle: OrderBundle = activeOrderBundles(orderBundleId)
    val orders: Seq[Order] = bundle.ordersRefs.flatMap(activeOrder)
    val finishTime = orders.map(_.lastUpdateTime).max

    val bill: OrderBill = OrderBill.calc(orders, tickers, config.tradeRoom.exchanges.map(e => (e._1, e._2.fee)))
    val finishedOrderBundle = FinishedOrderBundle(bundle, orders, finishTime, bill)
    this.synchronized {
      finishedOrderBundles = finishedOrderBundle :: finishedOrderBundles
    }
    activeOrderBundles.remove(orderBundleId)
    bundle.ordersRefs.foreach {
      e => activeOrders(e.exchange).remove(e)
    }

    clearLockedLiquidity(bundle.lockedLiquidity)

    if (bill.sumUSDTAtCalcTime >= 0) {
      val emoji = if (bill.sumUSDTAtCalcTime >= 1.0) Emoji.Opera else Emoji.Winning
      log.info(s"$emoji  ${finishedOrderBundle.shortDesc} completed with a win of ${formatDecimal(bill.sumUSDTAtCalcTime, 2)} USDT")
    } else {
      log.warn(s"${Emoji.SadFace}  ${finishedOrderBundle.shortDesc} completed with a loss of ${formatDecimal(bill.sumUSDTAtCalcTime, 2)} USDT ${Emoji.LookingDown}:\n $finishedOrderBundle")
    }
  }


  def cleanupLiquidityTxOrder(tx: LiquidityTx): Unit = {
    val order: Order = activeOrder(tx.orderRef).get // must exist
    val bill: OrderBill = OrderBill.calc(Seq(order), tickers, fees)
    val finishedLiquidityTx = FinishedLiquidityTx(tx, order, order.lastUpdateTime, bill)
    this.synchronized {
      finishedLiquidityTxs = finishedLiquidityTx :: finishedLiquidityTxs
    }
    activeLiquidityTx.remove(tx.orderRef)
    activeOrders(tx.orderRef.exchange).remove(tx.orderRef)
  }

  def cleanupFinishedOrderBundle(orderBundle: OrderBundle): Unit = {
    val orderBundleId: UUID = orderBundle.orderRequestBundle.id
    val orders: Seq[Order] = orderBundle.ordersRefs.flatMap(ref => activeOrder(ref))
    orders match {
      case order: Seq[Order] if order.isEmpty =>
        log.error(s"No order present for ${orderBundle.shortDesc} -> cleaning up")
        cleanupOrderBundle(orderBundleId)
      case order: Seq[Order] if order.forall(_.orderStatus == OrderStatus.FILLED) =>
        if (log.isTraceEnabled) log.trace(s"All orders of ${orderBundle.shortDesc} FILLED -> finishing it")
        cleanupOrderBundle(orderBundleId)
        log.info(s"${Emoji.Robot}  OrderBundle ${orderBundle.shortDesc} successfully finished")
      case order: Seq[Order] if order.forall(_.orderStatus.isFinal) =>
        log.debug(s"${Emoji.Robot}  All orders of ${orderBundle.shortDesc} have a final state (${order.map(_.orderStatus).mkString(",")}) -> not ideal")
        cleanupOrderBundle(orderBundleId)
        log.warn(s"${Emoji.Robot}  Finished OrderBundle ${orderBundle.shortDesc}, but NOT all orders are FILLED: $orders")
      case order: Seq[Order] => // order bundle still active: nothing to do
        if (log.isTraceEnabled) log.trace(s"Watching minor order update for $orderBundle: $order")
    }
  }

  def cleanupPossiblyFinishedLiquidityTxOrder(tx: LiquidityTx): Unit = {
    activeOrder(tx.orderRef) match {
      case Some(order) if order.orderStatus == OrderStatus.FILLED =>
        log.info(s"${Emoji.Robot}  Liquidity tx ${tx.orderRequest.tradeDesc} FILLED")
        cleanupLiquidityTxOrder(tx)
      case Some(order) if order.orderStatus.isFinal =>
        log.warn(s"${Emoji.NoSupport}  Liquidity tx ${tx.orderRef} finished with state ${order.orderStatus}")
        cleanupLiquidityTxOrder(tx)
      case Some(order) => // order still active: nothing to do
        if (log.isTraceEnabled) log.trace(s"Watching liquidity tx minor order update: $order")
    }
  }

  def onOrderUpdate(ref: OrderRef): Unit = {
    activeOrderBundles.values.find(e => e.ordersRefs.contains(ref)) match {
      case Some(orderBundle) =>
        cleanupFinishedOrderBundle(orderBundle)
        return
      case None => // proceed to next statement
    }

    activeLiquidityTx.get(ref) match {
      case Some(liquidityTx) => cleanupPossiblyFinishedLiquidityTxOrder(liquidityTx)
      case None => log.error(s"Got order-update (${ref.exchange}: ${ref.externalOrderId}) but cannot find active order bundle or liquidity tx with that id")
    }
  }

  def cancelAgedActiveOrders(): Unit = {
    val limit: Instant = Instant.now.minus(config.tradeRoom.maxOrderLifetime)

    val orderToCancel: Iterable[Order] =
      activeOrders.values.flatten
        .filterNot(_._2.orderStatus.isFinal)
        .filter(_._2.creationTime.isBefore(limit))
        .map(_._2)

    for (o: Order <- orderToCancel) {
      val source: String = activeLiquidityTx.values.find(_.orderRef.externalOrderId == o.externalId) match {
        case Some(liquidityTx) => s"liquidity-tx: ${liquidityTx.orderRequest.shortDesc}"
        case None => activeOrderBundles.values.find(_.ordersRefs.exists(_.externalOrderId == o.externalId)) match {
          case Some(orderBundle) => s"order-bundle: ${orderBundle.shortDesc}"
          case None =>
            log.warn(s"active $o not in our active-liquidity-tx or active-order-bundle list")
            "unknown source"
        }
      }

      log.warn(s"${Emoji.Judgemental}  Canceling aged order from $source")
      exchanges(o.exchange) ! CancelOrder(o.tradePair, o.externalId)
    }
  }


  def reportOrphanOpenOrders(): Unit = {
    activeOrders.values.foreach { orders =>
      val orphanOrders = orders
        .filterNot(o => activeOrderBundles.values.exists(_.ordersRefs.contains(o._1)))
        .filterNot(o => activeLiquidityTx.contains(o._1))
      if (orphanOrders.nonEmpty) {
        log.warn(s"""unreferenced order(s) on ${orphanOrders.head._1.exchange}:\n${orphanOrders.map(_._2.shortDesc).mkString(", ")}""")
        orphanOrders
          .filter(_._2.orderStatus.isFinal)
          .foreach { o =>
            log.info(s"cleanup finished orphan order ${o._2.shortDesc}")
            activeOrders(o._1.exchange).remove(o._1)
          }
      }
    }
  }

  def houseKeeping(): Unit = {
    cancelAgedActiveOrders()
    reportOrphanOpenOrders()

    // TODO report entries in openOrders, which are not referenced by activeOrderBundle or activeLiquidityTx - but what to do with them?
    // TODO report apparently dead entries in openOrderBundle
    // TODO report apparently dead entries in openLiquidityTx
  }


  def waitUntilAllOpenTxFinished(timeout: Duration): Unit = {
    val deadLine = Instant.now.plus(timeout)
    var openOrders: Iterable[Order] = null
    do {
      openOrders = activeOrders.values.flatten.map(_._2)
      if (openOrders.nonEmpty) {
        log.info(s"""${Emoji.Robot}  Waiting for ${openOrders.size} open orders: [${openOrders.map(_.shortDesc).mkString(", ")}]""")
        Thread.sleep(1000)
      }
    } while (Instant.now.isBefore(deadLine) && openOrders.nonEmpty)
    if (openOrders.nonEmpty) {
      log.warn(s"${Emoji.NoSupport}  These orders did not finish: [${openOrders.map(_.shortDesc).mkString(", ")}]")
    }
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 90.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  var exchangesJoined: Set[String] = Set()

  def startTraders(): Unit = {
    traders = traders + ("FooTrader" -> context.actorOf(FooTrader.props(config.trader("foo-trader"), self, tradeContext), "FooTrader"))
  }

  def onExchangeJoined(exchange: String): Unit = {
    exchangesJoined = exchangesJoined + exchange
    if (exchangesJoined == exchanges.keySet && traders.isEmpty) {
      log.info(s"${Emoji.Satisfied}  All exchanges initialized")
      startTraders()
    }
  }

  override def preStart(): Unit = {
    if (config.tradeRoom.tradeSimulation) log.info(s"Starting in trade simulation mode")
    else log.info(s"${Emoji.DoYouEvenLiftBro}  Starting in production mode")

    // TODO parallelize
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    for (exchange <- exchanges.values) {
      Await.ready(
        exchange ?
          JoinTradeRoom(
            self,
            (f: LiquidityTx => Boolean) => activeLiquidityTx.values.find(f),
            () => referenceTicker),
        timeout.duration.plus(1.second)
      )
    }
  }

  def onCancelOrderResult(c: CancelOrderResult): Unit = {
    if (c.success) {
      log.info(s"Cancel order succeeded: $c")
    } else {
      log.error(s"Cancel order failed: $c")
      // TODO error handling for failed cancels
    }
  }

  // @formatter:off
  def receive: Receive = {
    // messages from Exchanges
    case TradeRoomJoined(exchange)                  => onExchangeJoined(exchange)
    case bundle: OrderRequestBundle                 => tryToPlaceOrderBundle(bundle)
    case LiquidityTransformationOrder(orderRequest) => placeLiquidityTransformationOrder(orderRequest)
    case OrderUpdateTrigger(orderRef)               => onOrderUpdate(orderRef)
    case c: CancelOrderResult                       => onCancelOrderResult(c)
    case LogStats()                                 => logStats()
    case HouseKeeping()                             => houseKeeping()
    case Stop(timeout)                              => shutdown(timeout)
    case Status.Failure(cause)                      => log.error("received failure", cause)
  }
  // @formatter:on

  def shutdown(timeout: Duration): Unit = {
    log.info("shutdown initiated")
    shutdownInitiated = true
    exchanges.values.foreach {
      _ ! Stop(timeout.minusSeconds(2))
    }
    waitUntilAllOpenTxFinished(timeout.minusSeconds(2))
    sender() ! Done
    self ! PoisonPill
  }
}

// TODO single finished OrderBundle proof-of-concept check: If real win is a loss, we stop our application directly!
// TODO finished orderbundle statistics: Last hour: number of trades, Estimated Win, real win at tx times, real win now
// TODO decouple reference-ticker delivery from ExchangeTPDataManager (=active trade pairs), because Tradepair cleanup works against a rich reference ticker