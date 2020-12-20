package org.purevalue.arbitrage.traderoom

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop, Signal}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.{FooTrader, TemporaryLowDetector}
import org.purevalue.arbitrage.traderoom.OrderSetPlacer.NewOrderSet
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityLockRequest}
import org.purevalue.arbitrage.traderoom.exchange.{DataAge, Exchange, LiquidityBalancerStats, LiquidityManager, OrderBook, Stats24h, Ticker, TickerSnapshot, Wallet}
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.slf4j.LoggerFactory

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


case class TradeContext(tradePairs: Map[String, Set[TradePair]],
                        tickers: Map[String, Map[TradePair, Ticker]],
                        orderBooks: Map[String, Map[TradePair, OrderBook]],
                        stats24h: Map[String, Map[TradePair, Stats24h]],
                        dataAge: Map[String, DataAge],
                        wallets: Map[String, Wallet],
                        referenceTickerExchange: String,
                        feeRates: Map[String, Double],
                        doNotTouch: Map[String, Set[Asset]],
                        activeOrderBundleOrders: Iterable[OrderRef]) {
  def referenceTicker: Map[TradePair, Ticker] = tickers(referenceTickerExchange)
}

object TradeRoom {
  def apply(config: Config,
            exchanges: Map[String, ActorRef[Exchange.Message]]):
  Behavior[Message] = {
    Behaviors.withTimers(timers =>
      Behaviors.setup(context => new TradeRoom(context, timers, config, exchanges)))
  }

  sealed trait Message
  final case class OrderRef(exchange: String, pair: TradePair, externalOrderId: String)
  final case class OrderBundle(orderRequestBundle: OrderRequestBundle,
                               lockedLiquidity: Seq[LiquidityLock],
                               orderRefs: Seq[OrderRef]) {
    def shortDesc: String = s"OrderBundle(${orderRequestBundle.id}, ${orderRequestBundle.tradeDesc})"
  }
  final case class FinishedOrderBundle(bundle: OrderBundle,
                                       finishedOrders: Seq[Order],
                                       finishTime: Instant,
                                       bill: OrderBill) {
    def shortDesc: String = s"FinishedOrderBundle(${finishedOrders.map(o => o.shortDesc).mkString(" & ")})"
  }
  final case class LiquidityTx(orderRequest: OrderRequest,
                               orderRef: OrderRef,
                               lockedLiquidity: LiquidityLock,
                               creationTime: Instant)
  final case class FinishedLiquidityTx(liquidityTx: LiquidityTx,
                                       finishedOrder: Order,
                                       finishTime: Instant,
                                       bill: OrderBill)

  case class FullDataSnapshot(exchange: String,
                              usableTradePairs: Set[TradePair],
                              ticker: Map[TradePair, Ticker],
                              orderBook: Map[TradePair, OrderBook],
                              stats24h: Map[TradePair, Stats24h],
                              dataAge: DataAge,
                              wallet: Wallet)

  // communication
  case class GetReferenceTicker(replyTo: ActorRef[TickerSnapshot]) extends Message
  case class LogStats() extends Message
  case class HouseKeeping() extends Message
  case class OrderUpdateTrigger(ref: OrderRef, resendCounter: Int = 0) extends Message // status of an order has changed
  case class TriggerTrader() extends Message
  case class PlaceLiquidityTransformationOrder(orderRequest: OrderRequest, replyTo: ActorRef[Option[OrderRef]]) extends Message
  case class GetFinishedLiquidityTxs(replyTo: ActorRef[Set[OrderRef]]) extends Message
  case class TradeRoomJoined(exchange: String) extends Message
  case class PlaceOrderRequestBundle(bundle: OrderRequestBundle) extends Message
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom(context: ActorContext[TradeRoom.Message],
                timers: TimerScheduler[TradeRoom.Message],
                config: Config,
                exchanges: Map[String, ActorRef[Exchange.Message]])
  extends AbstractBehavior[TradeRoom.Message](context) {

  private val log = LoggerFactory.getLogger(getClass)

  import TradeRoom._

  private implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = system.executionContext

  private val orderBundleSafetyGuard = new OrderBundleSafetyGuard(config)

  private val logScheduleRate: FiniteDuration = FiniteDuration(config.tradeRoom.statsReportInterval.toNanos, TimeUnit.NANOSECONDS)
  timers.startTimerAtFixedRate(LogStats(), logScheduleRate)

  private var tradersStarted: Boolean = false
  private var fooTrader: ActorRef[FooTrader.Command] = _
  private var temporaryLowDetector: ActorRef[TemporaryLowDetector.Command] = _

  private val feeRates: Map[String, Double] = config.exchanges.values.map(e => (e.name, e.feeRate)).toMap // TODO query from exchange
  private val doNotTouchAssets: Map[String, Set[Asset]] = config.exchanges.values.map(e => (e.name, e.doNotTouchTheseAssets)).toMap

  // housekeeping of our requests: OrderBundles + LiquidityTx
  private val activeOrderBundles: collection.concurrent.Map[UUID, OrderBundle] = TrieMap()
  private val activeLiquidityTx: collection.concurrent.Map[OrderRef, LiquidityTx] = TrieMap()
  @volatile private var finishedOrderBundles: List[FinishedOrderBundle] = List()
  private val finishedLiquidityTxs: collection.concurrent.Map[OrderRef, FinishedLiquidityTx] = TrieMap()

  def collectTradeContext(): Future[TradeContext] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem

    val publicData: Iterable[Future[FullDataSnapshot]] = {
      exchanges.values.map(_.ask(ref => GetFullDataSnapshot(ref)))
    }

    Future.sequence(publicData).map(d =>
      TradeContext(
        d.map(e => e.exchange -> e.usableTradePairs).toMap,
        d.map(e => e.exchange -> e.ticker).toMap,
        d.map(e => e.exchange -> e.orderBook).toMap,
        d.map(e => e.exchange -> e.stats24h).toMap,
        d.map(e => e.exchange -> e.dataAge).toMap,
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
  def lockAllRequiredLiquidity(tradePattern: String, coins: Seq[LocalCryptoValue], dontUseTheseReserveAssets: Set[Asset]):
  Future[Option[List[LiquidityLock]]] = {

    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    Future.sequence(
      coins
        .groupBy(_.exchange)
        .map { x =>
          exchanges(x._1).ask((ref: ActorRef[Option[LiquidityLock]]) =>
            LiquidityLockRequest(
              UUID.randomUUID(),
              Instant.now(),
              x._1,
              tradePattern,
              x._2.map(c => CryptoValue(c.asset, c.amount)),
              isForLiquidityTx = false,
              dontUseTheseReserveAssets,
              None,
              ref
            ))
        }
    ).map {
      case x if x.forall(_.isDefined) =>
        Some(x.flatten.toList)
      case x => // release all other locks in case of partial non-success
        x.filter(_.isDefined).flatten.foreach { e =>
          exchanges(e.exchange) ! LiquidityLockClearance(e.liquidityRequestId)
        }
        None
    }
  }

  def placeOrders(orderRequests: Seq[OrderRequest]): Future[Seq[OrderRef]] = {
    implicit val timeout: Timeout = config.global.httpTimeout.mul(2) // covers parallel order request + possible order cancel operations

    context.spawn(OrderSetPlacer(config.global, exchanges), s"OrderSetPlacer-${UUID.randomUUID()}")
      .ask((ref: ActorRef[Seq[NewOrderAck]]) => NewOrderSet(orderRequests, ref))
      .map(_.map(_.toOrderRef))
  }

  def tryToPlaceLiquidityTransformationOrder(request: OrderRequest): Future[Option[OrderRef]] = {
    if (doNotTouchAssets(request.exchange).intersect(request.pair.involvedAssets).nonEmpty) return Future.failed(new IllegalArgumentException)

    // this should not occur - but here is a last guard
    if (activeLiquidityTx.keys.exists(ref => ref.exchange == request.exchange && ref.pair == request.pair)) {
      log.warn(s"Ignoring liquidity tx because a similar one (same trade pair on same exchange) is still in place: $request")
      return Future.successful(None)
    }

    implicit val timeout: Timeout = config.global.httpTimeout.plus(config.global.internalCommunicationTimeout.duration)
    val tradePattern = s"${request.exchange}-liquidityTx"
    exchanges(request.exchange).ask((ref: ActorRef[Option[LiquidityLock]]) =>
      LiquidityManager.LiquidityLockRequest(
        UUID.randomUUID(),
        Instant.now,
        request.exchange,
        tradePattern,
        Seq(request.calcOutgoingLiquidity.cryptoValue),
        isForLiquidityTx = true,
        Set(),
        None,
        ref
      )).flatMap {
      case Some(lock) =>
        exchanges(request.exchange)
          .ask((ref: ActorRef[NewOrderAck]) => NewLimitOrder(request, ref))
          .map { newOrderAck =>
          val ref: OrderRef = newOrderAck.toOrderRef
          activeLiquidityTx.update(ref, LiquidityTx(request, ref, lock, Instant.now))
          log.debug(s"successfully placed liquidity tx order $newOrderAck")
          Some(ref)
        } recover {
          case e: Exception =>
            log.error(s"failed to place new liquidity tx $request", e)
            exchanges(request.exchange) ! LiquidityLockClearance(lock.liquidityRequestId)
            None
        }

      case None =>
        log.debug(s"could not acquire lock for liquidity tx")
        Future.successful(None)
    } recover {
      case e: Throwable =>
        log.error("LiquidityLockRequest failed", e)
        None
    }
  }

  def registerOrderBundle(b: OrderRequestBundle, lockedLiquidity: Seq[LiquidityLock], orders: Seq[OrderRef]) {
    activeOrderBundles.update(b.id, OrderBundle(b, lockedLiquidity, orders))
  }

  def tryToPlaceOrderBundle(bundle: OrderRequestBundle): Unit = {
    if (bundle.orderRequests.exists(e =>
      doNotTouchAssets(e.exchange).intersect(e.pair.involvedAssets).nonEmpty)) {
      log.warn(s"ignoring $bundle containing a DO-NOT-TOUCH asset")
    }

    collectTradeContext().onComplete {
      case Success(tc: TradeContext) =>
        val isSafe: (Boolean, Option[Double]) = orderBundleSafetyGuard.isSafe(bundle)(tc)
        if (isSafe._1) {
          val totalWin: Double = isSafe._2.get
          val requiredLiquidity: Seq[LocalCryptoValue] = bundle.orderRequests.map(_.calcOutgoingLiquidity)

          lockAllRequiredLiquidity(bundle.tradePattern, requiredLiquidity, bundle.involvedReserveAssets) onComplete {

            case Success(Some(lockedLiquidity)) =>
              placeOrders(bundle.orderRequests) onComplete {
                case Success(orderRefs: Seq[OrderRef]) =>
                  registerOrderBundle(bundle, lockedLiquidity, orderRefs)
                  log.info(s"${Emoji.Excited}  Placed checked $bundle (estimated total win: ${formatDecimal(totalWin, 2)})")

                case Failure(e) => log.error("placing orders failed", e)
              }

            case Success(None) => log.info(s"""${Emoji.Robot}  Liquidity for trades not yet available: ${requiredLiquidity.mkString(", ")}""")
            case Failure(e) => log.error("lockAllRequiredLiquidity failed", e)
          }
        }
      case Failure(e) => log.error("collectTradeContext failed", e)
    }
  }

  // TODO probably we will need a merged reference-ticker which comes from binance + assets from other exchanges, which binance does not have

  def logStats(): Unit = {
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

      log.info(
        s"""${Emoji.Robot}  Last 1h final order status: trader tx:[${orderStateStats(orderBundleOrders1h).mkString(",")}],
           |liquidity tx: [${orderStateStats(liquidityTxOrders1h).mkString(",")}]""".stripMargin)
    }

    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val wf: Iterable[Future[Wallet]] = exchanges.values.map(e => e.ask(ref => GetWallet(ref)))
    val tf: Iterable[Future[TickerSnapshot]] = exchanges.values.map(e => e.ask(ref => GetTickerSnapshot(ref)))
    val rtf: Future[TickerSnapshot] = pullReferenceTicker
    (for {
      wallets <- Future.sequence(wf)
      tickers <- Future.sequence(tf)
      referenceTicker <- rtf
    } yield (
      wallets.map(e => e.exchange -> e).toMap,
      tickers.map(e => e.exchange -> e.ticker).toMap,
      referenceTicker.ticker
    )).onComplete {
      case Success((wallets, tickers, referenceTicker)) =>
        logWalletOverview(wallets, tickers)
        logOrderBundleSafetyGuardStats()
        logFinalOrderStateStats()
        logOrderGainStats(referenceTicker)
      case Failure(e) =>
        log.warn("get wallets/tickers/referenceTicker failed", e)
    }
  }

  def activeOrder(orderRef: OrderRef): Future[Option[Order]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    exchanges(orderRef.exchange).ask(ref => GetActiveOrders(ref)).map(_.get(orderRef))
  }

  // cleanup completed order bundle, which is still in the "active" list
  def cleanupOrderBundle(orderBundleId: UUID): Unit = {

    activeOrderBundles.synchronized { // to avoid, that the cleanup-code of another OrderUpdateTrigger of the same order-bundle is race-conditioning with us

      val bundle: OrderBundle = activeOrderBundles.get(orderBundleId) match {
        case Some(bundle) => bundle
        case None =>
          log.warn(s"OrderBundle with ID=$orderBundleId is gone")
          return
      }
      val of: Seq[Future[Option[Order]]] = bundle.orderRefs.map(e => activeOrder(e))
      val rtf = pullReferenceTicker
      (for {
        orders <- Future.sequence(of)
        referenceTicker <- rtf
      } yield (orders.flatten, referenceTicker.ticker)).onComplete {
        case Success((orders, referenceTicker)) =>
          val finishTime = orders.map(_.lastUpdateTime).max
          val usdEquivalentCoin: Asset = config.exchanges(config.tradeRoom.referenceTickerExchange).usdEquivalentCoin
          val bill: OrderBill = OrderBill.calc(orders, referenceTicker, usdEquivalentCoin, config.exchanges.map(e => (e._1, e._2.feeRate)))
          val finishedOrderBundle = FinishedOrderBundle(bundle, orders, finishTime, bill)
          finishedOrderBundles = finishedOrderBundle :: finishedOrderBundles
          activeOrderBundles.remove(orderBundleId)
          bundle.orderRefs.foreach {
            e => exchanges(e.exchange) ! RemoveActiveOrder(e)
          }

          bundle.lockedLiquidity.foreach { l =>
            exchanges(l.exchange) ! LiquidityLockClearance(l.liquidityRequestId)
          }

          if (orders.exists(_.orderStatus != OrderStatus.FILLED)) {
            log.warn(s"${Emoji.Questionable}  ${finishedOrderBundle.shortDesc} did not complete. Orders: \n${orders.mkString("\n")}")
          } else if (bill.sumUSDAtCalcTime >= 0) {
            val emoji = if (bill.sumUSDAtCalcTime >= 1.0) Emoji.Opera else Emoji.Winning
            log.info(s"$emoji  ${finishedOrderBundle.shortDesc} completed with a win of ${formatDecimal(bill.sumUSDAtCalcTime, 2)} USD")
          } else {
            log.warn(s"${Emoji.SadFace}  ${finishedOrderBundle.shortDesc} completed with a loss of ${formatDecimal(bill.sumUSDAtCalcTime, 2)} USD ${Emoji.LookingDown}:\n $finishedOrderBundle")
          }

        case Failure(e) => log.error("get orders/referenceTicker failed", e)
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
    } yield (order.get, referenceTicker.ticker)).onComplete {
      case Success((order, referenceTicker)) =>
        val usdEquivalentCoin: Asset = config.exchanges(order.exchange).usdEquivalentCoin
        val bill: OrderBill = OrderBill.calc(Seq(order), referenceTicker, usdEquivalentCoin, feeRates)
        val finishedLiquidityTx = FinishedLiquidityTx(tx, order, order.lastUpdateTime, bill)
        finishedLiquidityTxs.update(finishedLiquidityTx.liquidityTx.orderRef, finishedLiquidityTx)
        activeLiquidityTx.remove(tx.orderRef)
        exchanges(tx.orderRef.exchange) ! RemoveActiveOrder(tx.orderRef)
        exchanges(tx.lockedLiquidity.exchange) ! LiquidityLockClearance(tx.lockedLiquidity.liquidityRequestId)
      case Failure(e) => log.error(s"get order/referenceTicker failed", e)
    }
  }

  def cleanupPossiblyFinishedOrderBundle(orderBundle: OrderBundle): Unit = {
    val f: Seq[Future[Option[Order]]] = orderBundle.orderRefs.map(activeOrder)
    Future.sequence(f).onComplete {
      case Success(orders) =>
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
            if (log.isDebugEnabled) log.debug(s"${Emoji.Robot}  All orders of ${orderBundle.shortDesc} have a final state (${order.map(_.orderStatus).mkString(",")}) -> not ideal")
            cleanupOrderBundle(orderBundleId)
            log.warn(s"${Emoji.Robot}  Finished OrderBundle ${orderBundle.shortDesc}, but NOT all orders are FILLED: $orders")
          case order: Seq[Order] => // order bundle still active: nothing to do
            if (log.isDebugEnabled) log.debug(s"Watching minor order update for $orderBundle: $order")
        }
      case Failure(e) => log.error(s"get activeOrder failed", e)
    }
  }

  def cleanupPossiblyFinishedLiquidityTxOrder(tx: LiquidityTx): Unit = {
    activeOrder(tx.orderRef).onComplete {
      case Success(None) => log.error(s"order ${tx.orderRef} does not exist!")
      case Success(Some(order)) =>
        if (order.orderStatus == OrderStatus.FILLED) {
          log.info(s"${Emoji.Robot}  Liquidity tx ${tx.orderRequest.tradeDesc} (externalId:${tx.orderRef.externalOrderId}) FILLED")
          cleanupLiquidityTxOrder(tx)
        }
        else if (order.orderStatus.isFinal) {
          log.warn(s"${Emoji.NoSupport}  Liquidity tx ${tx.orderRef} finished with state ${order.orderStatus}")
          cleanupLiquidityTxOrder(tx)
        }
        else { // order still active: nothing to do
          if (log.isTraceEnabled) log.trace(s"Watching liquidity tx minor order update: $order")
        }
      case Failure(e) => log.error(s"get activOrder(${tx.orderRef}) failed", e)
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
          Future(concurrent.blocking {
            Thread.sleep(200)
            context.self ! OrderUpdateTrigger(t.ref, t.resendCounter + 1)
          })
        } else {
          activeOrder(t.ref).onComplete {
            case Success(None) => // order & liquidityTx gone -> nothing there to pay heed to
            case Success(Some(order)) =>
              log.warn(s"Got order-update (${t.ref.exchange}: ${t.ref.externalOrderId}) but cannot find active order bundle or liquidity tx for it." +
                s" Corresponding order is: $order")
            // otherwise, when the active order is already gone, we can just drop that update-trigger, because it comes too late.
            // Then the order from activeOrderBundles/activeLiquidityTx was already cleaned-up by a previous trigger
            case Failure(e) => log.error(s"activeOrder(${t.ref}) failed", e)
          }
        }
    }
  }

  def cancelAgedActiveOrders(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val f = exchanges.values.map(e => e.ask(ref => GetActiveOrders(ref)).map(_.values))
    Future.sequence(f).onComplete {
      case Success(allActiveOrders) =>
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

          log.warn(s"${Emoji.Judgemental}  Canceling aged order ${o.shortDesc} $source")
          exchanges(o.exchange) ! CancelOrder(o.ref, None)
        }

      case Failure(e) => log.error("GetActiveOrders failed", e)
    }
  }


  def reportOrphanOpenOrders(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val f = exchanges.values.map(e => e.ask(ref => GetActiveOrders(ref)))
    Future.sequence(f).onComplete {
      case Success(allActiveOrders) =>
        val orphanOrders = allActiveOrders.flatten
          .filterNot(o => activeOrderBundles.values.exists(_.orderRefs.contains(o._1)))
          .filterNot(o => activeLiquidityTx.contains(o._1))
        if (orphanOrders.nonEmpty) {
          log.warn(s"""unreferenced order(s) on ${orphanOrders.head._1.exchange}: ${orphanOrders.map(_._2.shortDesc).mkString(", ")}""")
          orphanOrders
            .filter(_._2.orderStatus.isFinal)
            .foreach { o =>
              exchanges(o._1.exchange) ! RemoveOrphanOrder(o._1)
            }
        }

      case Failure(e) => log.error("GetActiveOrders failed", e)
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
    fooTrader = context.spawn(FooTrader(Config.trader("foo-trader"), context.self), "FooTrader")
    temporaryLowDetector = context.spawn(TemporaryLowDetector(config.exchanges), "TemporaryLowDetector")

    val traderScheduleDelay: FiniteDuration = FiniteDuration(config.tradeRoom.traderTriggerInterval.toMillis, TimeUnit.MILLISECONDS)
    timers.startTimerWithFixedDelay(TriggerTrader(), traderScheduleDelay)
    tradersStarted = true
  }

  def onExchangeJoined(exchange: String): Unit = {
    exchangesJoined = exchangesJoined + exchange
    if (exchangesJoined == exchanges.keySet && !tradersStarted) {
      log.info(s"${Emoji.Satisfied}  All exchanges initialized")
      timers.startTimerAtFixedRate(HouseKeeping(), 3.seconds)
      startTraders()
    }
  }

  def pullReferenceTicker: Future[TickerSnapshot] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    exchanges(config.tradeRoom.referenceTickerExchange)
      .ask(ref => GetTickerSnapshot(ref))
      .recover {
        case e: Throwable =>
          log.error("GetTickerSnapshot failed", e)
          throw e
      }
  }

  def triggerTrader(): Unit = {
    collectTradeContext().onComplete {
      case Success(tc) =>
        fooTrader ! FooTrader.SearchRun(tc)
        temporaryLowDetector ! TemporaryLowDetector.SearchRun(tc)
      case Failure(e) => log.error("collectTradeContext failed", e)
    }
  }

  override def onMessage(message: Message): Behavior[Message] = {
    message match {
      // @formatter:off
      case TradeRoomJoined(exchange)                                => onExchangeJoined(exchange)
      case PlaceOrderRequestBundle(bundle)                          => tryToPlaceOrderBundle(bundle)
      case PlaceLiquidityTransformationOrder(orderRequest, replyTo) => tryToPlaceLiquidityTransformationOrder(orderRequest).foreach(r => replyTo ! r)

      case t: OrderUpdateTrigger                                    => onOrderUpdate(t)

      case GetReferenceTicker(replyTo)                              => pullReferenceTicker.foreach(replyTo ! _)
      case GetFinishedLiquidityTxs(replyTo)                         => replyTo ! finishedLiquidityTxs.keySet.toSet

      case LogStats()                                               => logStats()
      case HouseKeeping()                                           => houseKeeping()
      case TriggerTrader()                                          => triggerTrader()
      // @formatter:on
    }
    this
  }

  override def onSignal: PartialFunction[Signal, Behavior[Message]] = {
    case PostStop =>
      log.info("TradeRoom stopped")
      this
  }

  if (config.tradeRoom.tradeSimulation) log.info(s"Starting in trade simulation mode")
  else log.info(s"${Emoji.DoYouEvenLiftBro}  Starting in production mode")

  for (exchange <- exchanges.values) {
    exchange ! Exchange.JoinTradeRoom(context.self)
  }
}

// TODO single finished OrderBundle proof-of-concept check: If real win is a loss, we stop our application directly!
// TODO finished orderbundle statistics: Last hour: number of trades, Estimated Win, real win at tx times, real win now
// TODO decouple reference-ticker delivery from ExchangeTPDataManager (=active trade pairs), because Tradepair cleanup works against a rich reference ticker