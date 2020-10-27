package org.purevalue.arbitrage.traderoom

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.FooTrader
import org.purevalue.arbitrage.traderoom.OrderSetPlacer.NewOrderSet
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityLockRequest}
import org.purevalue.arbitrage.traderoom.exchange.{FullDataSnapshot, LiquidityBalancerStats, OrderBook, Ticker, TickerSnapshot, Wallet}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.util.{Emoji, WrongAssumption}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


case class TradeContext(tradePairs: Map[String, Set[TradePair]],
                        tickers: Map[String, Map[TradePair, Ticker]],
                        orderBooks: Map[String, Map[TradePair, OrderBook]],
                        heartbeatTS: Map[String, Option[Instant]],
                        tickerTS: Map[String, Option[Instant]],
                        orderBooksTS: Map[String, Option[Instant]],
                        wallets: Map[String, Wallet],
                        referenceTickerExchange: String,
                        feeRates: Map[String, Double],
                        doNotTouch: Map[String, Set[Asset]],
                        activeOrderBundleOrders: Iterable[OrderRef]) {
  def referenceTicker: Map[TradePair, Ticker] = tickers(referenceTickerExchange)
}

object TradeRoom {

  case class OrderRef(exchange: String, tradePair: TradePair, externalOrderId: String)

  case class OrderBundle(orderRequestBundle: OrderRequestBundle,
                         lockedLiquidity: Seq[LiquidityLock],
                         orderRefs: Seq[OrderRef]) {
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
                         lockedLiquidity: LiquidityLock,
                         creationTime: Instant)
  case class FinishedLiquidityTx(liquidityTx: LiquidityTx,
                                 finishedOrder: Order,
                                 finishTime: Instant,
                                 bill: OrderBill)

  // communication
  case class GetReferenceTicker()
  case class LogStats()
  case class HouseKeeping()
  case class OrderUpdateTrigger(ref: OrderRef, resendCounter: Int = 0) // status of an order has changed
  case class TriggerTrader()
  case class Stop()
  case class NewLiquidityTransformationOrder(orderRequest: OrderRequest)
  case class GetFinishedLiquidityTxs()
  case class JoinTradeRoom(tradeRoom: ActorRef)
  case class TradeRoomJoined(exchange: String)

  def props(config: Config,
            exchanges: Map[String, ActorRef],
            usableTradePairs: Map[String, Set[TradePair]]): Props =
    Props(new TradeRoom(config, exchanges, usableTradePairs))
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom(val config: Config,
                val exchanges: Map[String, ActorRef],
                val usableTradePairs: Map[String, Set[TradePair]]) extends Actor with ActorLogging {

  private implicit val actorSystem: ActorSystem = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private val orderBundleSafetyGuard = new OrderBundleSafetyGuard(config)

  private val houseKeepingSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(5.seconds, 3.seconds, self, HouseKeeping())
  private val logScheduleRate: FiniteDuration = FiniteDuration(config.tradeRoom.statsReportInterval.toNanos, TimeUnit.NANOSECONDS)
  private val logSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(2.minutes, logScheduleRate, self, LogStats())

  private val traderScheduleDelay: FiniteDuration = FiniteDuration(config.tradeRoom.traderTriggerInterval.toNanos, TimeUnit.NANOSECONDS)
  private var traderSchedule: Cancellable = _

  private var fooTrader: Option[ActorRef] = None

  private val feeRates: Map[String, Double] = config.exchanges.values.map(e => (e.name, e.feeRate)).toMap // TODO query from exchange
  private val doNotTouchAssets: Map[String, Set[Asset]] = config.exchanges.values.map(e => (e.name, e.doNotTouchTheseAssets)).toMap

  // housekeeping of our requests: OrderBundles + LiquidityTx
  private val activeOrderBundles: collection.concurrent.Map[UUID, OrderBundle] = TrieMap()
  private val activeLiquidityTx: collection.concurrent.Map[OrderRef, LiquidityTx] = TrieMap()
  @volatile private var finishedOrderBundles: List[FinishedOrderBundle] = List()
  private val finishedLiquidityTxs: collection.concurrent.Map[OrderRef, FinishedLiquidityTx] = TrieMap()

  private var shutdownInitiated: Boolean = false


  def collectTradeContext(): Future[TradeContext] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val publicData: Iterable[Future[FullDataSnapshot]] = exchanges.values.map(e => (e ? GetFullDataSnapshot()).mapTo[FullDataSnapshot])
    Future.sequence(publicData).map(d =>
      TradeContext(
        usableTradePairs,
        d.map(e => e.exchange -> e.ticker).toMap,
        d.map(e => e.exchange -> e.orderBook).toMap,
        d.map(e => e.exchange -> e.heartbeatTS).toMap,
        d.map(e => e.exchange -> e.tickerTS).toMap,
        d.map(e => e.exchange -> e.orderBookTS).toMap,
        d.map(e => e.exchange -> e.wallet).toMap,
        config.tradeRoom.referenceTickerExchange,
        feeRates,
        config.exchanges.map(e => e._1 -> e._2.doNotTouchTheseAssets),
        activeOrderBundles.flatMap(_._2.orderRefs))
    )
  }

  /**
   * Lock liquidity
   *
   * @return all (locked=true) or nothing
   */
  def lockAllRequiredLiquidity(tradePattern: String, coins: Seq[LocalCryptoValue], dontUseTheseReserveAssets: Set[Asset]): Future[Option[List[LiquidityLock]]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    Future.sequence(
      coins
        .groupBy(_.exchange)
        .map { x =>
          (exchanges(x._1) ?
            LiquidityLockRequest(
              UUID.randomUUID(),
              Instant.now(),
              x._1,
              tradePattern,
              x._2.map(c => CryptoValue(c.asset, c.amount)),
              isForLiquidityTx = false,
              dontUseTheseReserveAssets))
            .mapTo[Option[LiquidityLock]]
        }
    ).map {
      case x if x.forall(_.isDefined) =>
        Some(x.flatten.toList)
      case x => // release all other locks in case of partial non-success
        x.filter(_.isDefined).flatten.foreach { e =>
          exchanges(e.exchange) ! LiquidityLockClearance(e.liquidityRequestId)
        }
        None
    } recover {
      case e: Exception =>
        log.error(e, "Error while locking liquidity")
        None
    }
  }

  def placeOrders(orderRequests: List[OrderRequest]): Future[List[OrderRef]] = {
    implicit val timeout: Timeout = config.global.httpTimeout.mul(2) // covers parallel order request + possible order cancel operations

    (context.actorOf(OrderSetPlacer.props(exchanges)) ? NewOrderSet(orderRequests.map(o => NewLimitOrder(o))))
      .mapTo[List[NewOrderAck]]
      .map(_.map(_.toOrderRef))
  }

  def tryToPlaceLiquidityTransformationOrder(request: OrderRequest): Future[Option[OrderRef]] = {
    if (shutdownInitiated) return Future.successful(None)
    if (doNotTouchAssets(request.exchange).intersect(request.pair.involvedAssets).nonEmpty) return Future.failed(new IllegalArgumentException)

    // this should not occur - but here is a last guard
    if (activeLiquidityTx.keys.exists(ref => ref.exchange == request.exchange && ref.tradePair == request.pair)) {
      log.warning(s"Ignoring liquidity tx because a similar one (same trade pair on same exchange) is still in place: $request")
      return Future.successful(None)
    }

    val tradePattern = s"${request.exchange}-liquidityTx"
    val liquidityRequest =
      LiquidityLockRequest(
        UUID.randomUUID(),
        Instant.now,
        request.exchange,
        tradePattern,
        Seq(request.calcOutgoingLiquidity.cryptoValue),
        isForLiquidityTx = true,
        Set())

    implicit val timeout: Timeout = config.global.httpTimeout.plus(config.global.internalCommunicationTimeout.duration)
    (exchanges(request.exchange) ? liquidityRequest).mapTo[Option[LiquidityLock]].flatMap {

      case Some(lock) => (exchanges(request.exchange) ? NewLimitOrder(request)).mapTo[NewOrderAck].map {
        newOrderAck =>
          val ref: OrderRef = newOrderAck.toOrderRef
          activeLiquidityTx.update(ref, LiquidityTx(request, ref, lock, Instant.now))
          log.debug(s"successfully placed liquidity tx order $newOrderAck")
          Some(ref)
      } recover {
        case e: Exception =>
          log.error(e, s"failed to place new liquidity tx $request")
          exchanges(request.exchange) ? LiquidityLockClearance(lock.liquidityRequestId)
          None
      }

      case None =>
        log.debug(s"could not acquire lock for liquidity tx")
        Future.successful(None)
    }
  }

  def registerOrderBundle(b: OrderRequestBundle, lockedLiquidity: Seq[LiquidityLock], orders: Seq[OrderRef]) {
    activeOrderBundles.update(b.id, OrderBundle(b, lockedLiquidity, orders))
  }

  def tryToPlaceOrderBundle(bundle: OrderRequestBundle): Unit = {
    if (shutdownInitiated) return

    if (bundle.orderRequests.exists(e =>
      doNotTouchAssets(e.exchange).intersect(e.pair.involvedAssets).nonEmpty)) {
      log.warning(s"ignoring $bundle containing a DO-NOT-TOUCH asset")
    }

    collectTradeContext().foreach {
      tc: TradeContext =>
        val isSafe: (Boolean, Option[Double]) = orderBundleSafetyGuard.isSafe(bundle)(tc)
        if (isSafe._1) {
          val totalWin: Double = isSafe._2.get
          val requiredLiquidity: Seq[LocalCryptoValue] = bundle.orderRequests.map(_.calcOutgoingLiquidity)

          lockAllRequiredLiquidity(bundle.tradePattern, requiredLiquidity, bundle.involvedReserveAssets) onComplete {

            case Success(Some(lockedLiquidity)) =>
              placeOrders(bundle.orderRequests) onComplete {
                case Success(orderRefs: List[OrderRef]) =>
                  registerOrderBundle(bundle, lockedLiquidity, orderRefs)
                  log.info(s"${Emoji.Excited}  Placed checked $bundle (estimated total win: ${formatDecimal(totalWin, 2)})")

                case Failure(e) => log.error(e, "placing orders failed")
              }

            case Success(None) => log.info(s"""${Emoji.Robot}  Liquidity for trades not yet available: ${requiredLiquidity.mkString(", ")}""")
            case Failure(e) => log.error(e, "lockAllRequiredLiquidity failed")
          }
        }
    }
  }

  // TODO probably we will need a merged reference-ticker which comes from binance + assets from other exchanges, which binance does not have

  def logStats(): Unit = {
    if (shutdownInitiated) return
    val now = Instant.now

    def logWalletOverview(wallets: Map[String, Wallet], tickers: Map[String, Map[TradePair, Ticker]]): Unit = {
      wallets.values.foreach { w =>
        val walletOverview: String = w.toOverviewString(config.exchanges(w.exchange).usdEquivalentCoin, tickers(w.exchange))
        log.info(s"${Emoji.Robot}  $walletOverview")
      }
    }

    def logOrderBundleSafetyGuardStats(): Unit = {
      log.info(s"${Emoji.Robot}  OrderBundleSafetyGuard decision stats: [${orderBundleSafetyGuard.unsafeStats.mkString("|")}]")
    }

    def logOrderGainStats(referenceTicker: Map[TradePair, Ticker]): Unit = {
      val reportingUsdEquivalentCoin: Asset = config.exchanges(config.tradeRoom.referenceTickerExchange).usdEquivalentCoin
      val lastHourArbitrageSumUSD: Double =
        OrderBill.aggregateValues(
          finishedOrderBundles
            .filter(b => Duration.between(b.finishTime, now).toHours < 1)
            .flatMap(_.bill.balanceSheet),
          reportingUsdEquivalentCoin,
          (_, tp) => referenceTicker.get(tp).map(_.priceEstimate))

      val lastHourLiquidityTxSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedLiquidityTxs.values
            .filter(b => Duration.between(b.finishTime, now).toHours < 1)
            .flatMap(_.bill.balanceSheet),
          reportingUsdEquivalentCoin,
          (_, tp) => referenceTicker.get(tp).map(_.priceEstimate))

      val lastHourSumUSDT: Double = lastHourArbitrageSumUSD + lastHourLiquidityTxSumUSDT
      log.info(s"${Emoji.Robot}  Last 1h: cumulated gain: ${formatDecimal(lastHourSumUSDT, 2)} USD " +
        s"(arbitrage orders: ${formatDecimal(lastHourArbitrageSumUSD, 2)} USD, " +
        s"liquidity tx: ${formatDecimal(lastHourLiquidityTxSumUSDT, 2)} USD) ")

      val totalArbitrageSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedOrderBundles
            .flatMap(_.bill.balanceSheet),
          reportingUsdEquivalentCoin,
          (_, tp) => referenceTicker.get(tp).map(_.priceEstimate))
      val totalLiquidityTxSumUSDT: Double =
        OrderBill.aggregateValues(
          finishedLiquidityTxs.values
            .flatMap(_.bill.balanceSheet),
          reportingUsdEquivalentCoin,
          (_, tp) => referenceTicker.get(tp).map(_.priceEstimate))
      val totalSumUSDT: Double = totalArbitrageSumUSDT + totalLiquidityTxSumUSDT
      log.info(s"${Emoji.Robot}  Total cumulated gain: ${formatDecimal(totalSumUSDT, 2)} USD " +
        s"(arbitrage orders: ${formatDecimal(totalArbitrageSumUSDT, 2)} USD, liquidity tx: " +
        s"${formatDecimal(totalLiquidityTxSumUSDT, 2)} USD) ")

      LiquidityBalancerStats.logStats()
    }

    def logFinalOrderStateStats(): Unit = {
      def orderStateStats(orders: Iterable[Order]): Map[OrderStatus, Int] =
        orders
          .groupBy(_.orderStatus)
          .map(e => (e._1, e._2.size))

      val lastHourLimit = Instant.now.minus(1, ChronoUnit.HOURS)
      val liquidityTxOrders1h = finishedLiquidityTxs.values.filter(_.finishTime.isAfter(lastHourLimit)).map(_.finishedOrder)
      val orderBundleOrders1h = finishedOrderBundles.filter(_.finishTime.isAfter(lastHourLimit)).flatMap(_.finishedOrders)

      log.info(s"""${Emoji.Robot}  Last 1h final order status: trader tx:[${orderStateStats(orderBundleOrders1h).mkString(",")}], liquidity tx: [${orderStateStats(liquidityTxOrders1h).mkString(",")}]""")
    }

    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val wf: Iterable[Future[Wallet]] = exchanges.values.map(e => (e ? GetWallet()).mapTo[Wallet])
    val tf: Iterable[Future[TickerSnapshot]] = exchanges.values.map(e => (e ? GetTickerSnapshot()).mapTo[TickerSnapshot])
    val rtf: Future[TickerSnapshot] = pullReferenceTicker
    (for {
      wallets <- Future.sequence(wf)
      tickers <- Future.sequence(tf)
      referenceTicker <- rtf
    } yield (
      wallets.map(e => e.exchange -> e).toMap,
      tickers.map(e => e.exchange -> e.ticker).toMap,
      referenceTicker.ticker
    )).foreach {
      case (wallets, tickers, referenceTicker) =>
        logWalletOverview(wallets, tickers)
        logOrderBundleSafetyGuardStats()
        logFinalOrderStateStats()
        logOrderGainStats(referenceTicker)
    }
  }

  def activeOrder(ref: OrderRef): Future[Option[Order]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    (exchanges(ref.exchange) ? GetActiveOrder(ref)).mapTo[Option[Order]]
  }

  // cleanup completed order bundle, which is still in the "active" list
  def cleanupOrderBundle(orderBundleId: UUID): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val bundle: OrderBundle = activeOrderBundles.get(orderBundleId) match {
      case Some(bundle) => bundle
      case None =>
        log.error(s"OrderBundle with ID=$orderBundleId is gone") // TODO sequential processing here, to avoid that!!!
        return
    }
    val of: Seq[Future[Option[Order]]] = bundle.orderRefs.map(e => activeOrder(e))
    val rtf = pullReferenceTicker
    (for {
      orders <- Future.sequence(of)
      referenceTicker <- rtf
    } yield (orders.flatten, referenceTicker.ticker)).foreach {
      case (orders, referenceTicker) =>
        val finishTime = orders.map(_.lastUpdateTime).max
        val usdEquivalentCoin: Asset = config.exchanges(config.tradeRoom.referenceTickerExchange).usdEquivalentCoin
        val bill: OrderBill = OrderBill.calc(orders, referenceTicker, usdEquivalentCoin, config.exchanges.map(e => (e._1, e._2.feeRate)))
        val finishedOrderBundle = FinishedOrderBundle(bundle, orders, finishTime, bill)
        synchronized {
          finishedOrderBundles = finishedOrderBundle :: finishedOrderBundles
        }
        activeOrderBundles.remove(orderBundleId)
        bundle.orderRefs.foreach {
          e => exchanges(e.exchange) ! RemoveActiveOrder(e)
        }

        bundle.lockedLiquidity.foreach { l =>
          exchanges(l.exchange) ! LiquidityLockClearance(l.liquidityRequestId)
        }

        if (orders.exists(_.orderStatus != OrderStatus.FILLED)) {
          log.warning(s"${Emoji.Questionable}  ${finishedOrderBundle.shortDesc} did not complete. Orders: \n${orders.mkString("\n")}")
        } else if (bill.sumUSDAtCalcTime >= 0) {
          val emoji = if (bill.sumUSDAtCalcTime >= 1.0) Emoji.Opera else Emoji.Winning
          log.info(s"$emoji  ${finishedOrderBundle.shortDesc} completed with a win of ${formatDecimal(bill.sumUSDAtCalcTime, 2)} USD")
        } else {
          log.warning(s"${Emoji.SadFace}  ${finishedOrderBundle.shortDesc} completed with a loss of ${formatDecimal(bill.sumUSDAtCalcTime, 2)} USD ${Emoji.LookingDown}:\n $finishedOrderBundle")
        }
    }
  }


  def cleanupLiquidityTxOrder(tx: LiquidityTx): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val of = activeOrder(tx.orderRef)
    val rtf = pullReferenceTicker
    (for {
      order <- of
      referenceTicker <- rtf
    } yield (order.get, referenceTicker.ticker)).foreach {
      case (order, referenceTicker) =>
        val usdEquivalentCoin: Asset = config.exchanges(order.exchange).usdEquivalentCoin
        val bill: OrderBill = OrderBill.calc(Seq(order), referenceTicker, usdEquivalentCoin, feeRates)
        val finishedLiquidityTx = FinishedLiquidityTx(tx, order, order.lastUpdateTime, bill)
        finishedLiquidityTxs.update(finishedLiquidityTx.liquidityTx.orderRef, finishedLiquidityTx)
        activeLiquidityTx.remove(tx.orderRef)
        exchanges(tx.orderRef.exchange) ! RemoveActiveOrder(tx.orderRef)
        exchanges(tx.lockedLiquidity.exchange) ! LiquidityLockClearance(tx.lockedLiquidity.liquidityRequestId)
    }
  }

  def cleanupPossiblyFinishedOrderBundle(orderBundle: OrderBundle): Unit = {
    val f: Seq[Future[Option[Order]]] = orderBundle.orderRefs.map(activeOrder)
    Future.sequence(f).foreach { orders =>
      val orderBundleId: UUID = orderBundle.orderRequestBundle.id
      orders.flatten match {
        case order: Seq[Order] if order.isEmpty =>
          log.error(s"No order present for ${orderBundle.shortDesc} -> cleaning up")
          cleanupOrderBundle(orderBundleId)
        case order: Seq[Order] if order.forall(_.orderStatus == OrderStatus.FILLED) =>
          if (log.isDebugEnabled) log.debug(s"All orders of ${orderBundle.shortDesc} FILLED -> finishing it")
          cleanupOrderBundle(orderBundleId)
          log.info(s"${Emoji.Robot}  OrderBundle ${orderBundle.shortDesc} successfully finished")
        case order: Seq[Order] if order.forall(_.orderStatus.isFinal) =>
          log.debug(s"${Emoji.Robot}  All orders of ${orderBundle.shortDesc} have a final state (${order.map(_.orderStatus).mkString(",")}) -> not ideal")
          cleanupOrderBundle(orderBundleId)
          log.warning(s"${Emoji.Robot}  Finished OrderBundle ${orderBundle.shortDesc}, but NOT all orders are FILLED: $orders")
        case order: Seq[Order] => // order bundle still active: nothing to do
          if (log.isDebugEnabled) log.debug(s"Watching minor order update for $orderBundle: $order")
      }
    }
  }

  def cleanupPossiblyFinishedLiquidityTxOrder(tx: LiquidityTx): Unit = {
    activeOrder(tx.orderRef).foreach {
      case None => throw new WrongAssumption("order must exist")
      case Some(order) =>
        if (order.orderStatus == OrderStatus.FILLED) {
          log.info(s"${Emoji.Robot}  Liquidity tx ${tx.orderRequest.tradeDesc} (externalId:${tx.orderRef.externalOrderId}) FILLED")
          cleanupLiquidityTxOrder(tx)
        }
        else if (order.orderStatus.isFinal) {
          log.warning(s"${Emoji.NoSupport}  Liquidity tx ${tx.orderRef} finished with state ${order.orderStatus}")
          cleanupLiquidityTxOrder(tx)
        }
        else { // order still active: nothing to do
          if (log.isDebugEnabled) log.debug(s"Watching liquidity tx minor order update: $order")
        }
    }
  }

  def onOrderUpdate(t: OrderUpdateTrigger): Unit = {
    val MaxTries = 3

    activeOrderBundles.values.find(e => e.orderRefs.contains(t.ref)) match {
      case Some(orderBundle) =>
        cleanupPossiblyFinishedOrderBundle(orderBundle)
        return
      case None => // proceed to next statement
    }

    activeLiquidityTx.get(t.ref) match {
      case Some(liquidityTx) => cleanupPossiblyFinishedLiquidityTxOrder(liquidityTx)
      case None =>
        if (t.resendCounter < MaxTries) {
          // wait 200ms and try again, before giving up - sometimes the real order-filled-update is faster than our registering of the acknowledge
          Future (concurrent.blocking {
            Thread.sleep(200)
            self ! OrderUpdateTrigger(t.ref, t.resendCounter + 1)
          })
        } else {
          activeOrder(t.ref).foreach {
            case None => // order & liquidityTx gone -> nothing there to pay heed to
            case Some(order) =>
              log.warning(s"Got order-update (${t.ref.exchange}: ${t.ref.externalOrderId}) but cannot find active order bundle or liquidity tx for it." +
                s" Corresponding order is: $order")
            // otherwise, when the active order is already gone, we can just drop that update-trigger, because it comes too late.
            // Then the order from activeOrderBundles/activeLiquidityTx was already cleaned-up by a previous trigger
          }
        }
    }
  }

  def cancelAgedActiveOrders(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val f = exchanges.values.map(e => (e ? GetActiveOrders()).mapTo[Map[OrderRef, Order]].map(_.values))
    Future.sequence(f).foreach { allActiveOrders =>
      val limit: Instant = Instant.now.minus(config.tradeRoom.maxOrderLifetime)
      val orderToCancel: Iterable[Order] =
        allActiveOrders.flatten
          .filterNot(_.orderStatus.isFinal)
          .filter(_.creationTime.isBefore(limit))

      for (o: Order <- orderToCancel) {
        val source: String = activeLiquidityTx.values.find(_.orderRef.externalOrderId == o.externalId) match {
          case Some(liquidityTx) => s"from liquidity-tx: ${liquidityTx.orderRequest.shortDesc}"
          case None => activeOrderBundles.values.find(_.orderRefs.exists(_.externalOrderId == o.externalId)) match {
            case Some(orderBundle) => s"from order-bundle: ${orderBundle.shortDesc}"
            case None => "not referenced in order-bundles nor liquidity-tx"
          }
        }

        log.warning(s"${Emoji.Judgemental}  Canceling aged order ${o.shortDesc} $source")
        exchanges(o.exchange) ! CancelOrder(o.ref)
      }
    }
  }


  def reportOrphanOpenOrders(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val f = exchanges.values.map(e => (e ? GetActiveOrders()).mapTo[Map[OrderRef, Order]])
    Future.sequence(f).foreach { allActiveOrders =>
      val orphanOrders = allActiveOrders.flatten
        .filterNot(o => activeOrderBundles.values.exists(_.orderRefs.contains(o._1)))
        .filterNot(o => activeLiquidityTx.contains(o._1))
      if (orphanOrders.nonEmpty) {
        log.warning(s"""unreferenced order(s) on ${orphanOrders.head._1.exchange}: ${orphanOrders.map(_._2.shortDesc).mkString(", ")}""")
        orphanOrders
          .filter(_._2.orderStatus.isFinal)
          .foreach { o =>
            exchanges(o._1.exchange) ! RemoveOrphanOrder(o._1)
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


  var exchangesJoined: Set[String] = Set()

  def startTraders(): Unit = {
    fooTrader = Some(context.actorOf(FooTrader.props(Config.trader("foo-trader"), self), "FooTrader"))
    traderSchedule = actorSystem.scheduler.scheduleWithFixedDelay(3.minutes, traderScheduleDelay, self, TriggerTrader())
  }

  def onExchangeJoined(exchange: String): Unit = {
    exchangesJoined = exchangesJoined + exchange
    if (exchangesJoined == exchanges.keySet && fooTrader.isEmpty) {
      log.info(s"${Emoji.Satisfied}  All exchanges initialized")
      startTraders()
    }
  }

  override def preStart(): Unit = {
    if (config.tradeRoom.tradeSimulation) log.info(s"Starting in trade simulation mode")
    else log.info(s"${Emoji.DoYouEvenLiftBro}  Starting in production mode")

    for (exchange <- exchanges.values) {
      exchange ! JoinTradeRoom(self)
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

  def pullReferenceTicker: Future[TickerSnapshot] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    (exchanges(config.tradeRoom.referenceTickerExchange) ? GetTickerSnapshot()).mapTo[TickerSnapshot]
  }

  // @formatter:off
  override def receive: Receive = {
    // messages from Exchanges
    case TradeRoomJoined(exchange)                     => onExchangeJoined(exchange)
    case bundle: OrderRequestBundle                    => tryToPlaceOrderBundle(bundle)
    case NewLiquidityTransformationOrder(orderRequest) => tryToPlaceLiquidityTransformationOrder(orderRequest).pipeTo(sender())
    case t: OrderUpdateTrigger                         => onOrderUpdate(t)
    case c: CancelOrderResult                          => onCancelOrderResult(c)

    case GetReferenceTicker()                          => pullReferenceTicker.pipeTo(sender())
    case GetFinishedLiquidityTxs()                     => sender() ! finishedLiquidityTxs.keySet.toSet

    case LogStats()                                    => logStats()
    case HouseKeeping()                                => houseKeeping()
    case TriggerTrader()                               => collectTradeContext().pipeTo(fooTrader.get)
    case Stop()                                        => shutdown()
    case akka.actor.Status.Failure(cause)              => log.error(cause, "Failure received")
  }
  // @formatter:on

  def shutdown(): Unit = {
    log.info("shutdown initiated")
    shutdownInitiated = true
    exchanges.values.foreach {
      _ ! Stop()
    }
    sender() ! Done
    self ! PoisonPill
  }
}

// TODO single finished OrderBundle proof-of-concept check: If real win is a loss, we stop our application directly!
// TODO finished orderbundle statistics: Last hour: number of trades, Estimated Win, real win at tx times, real win now
// TODO decouple reference-ticker delivery from ExchangeTPDataManager (=active trade pairs), because Tradepair cleanup works against a rich reference ticker