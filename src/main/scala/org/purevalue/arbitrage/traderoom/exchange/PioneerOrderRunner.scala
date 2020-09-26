package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, CancelOrderResult, NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.adapter.{ExchangeAccountData, ExchangePublicData, Fee}
import org.purevalue.arbitrage.traderoom.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrder, PioneerOrderFailed, PioneerOrderSucceeded, Watch}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.util.{InitSequence, InitStep, Util, WaitingFor}
import org.purevalue.arbitrage.{GlobalConfig, Main, TradeRoomConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


// run some validated pioneer orders before the exchange gets available for further orders
object PioneerOrderRunner {
  case class Watch()
  case class PioneerOrderSucceeded()
  case class PioneerOrderFailed(exception: Throwable)

  case class PioneerOrder(request: OrderRequest, ref: OrderRef)

  def props(globalConfig: GlobalConfig,
            tradeRoomConfig: TradeRoomConfig,
            exchangeName: String,
            exchange: ActorRef,
            accountData: ExchangeAccountData,
            publicData: ExchangePublicData): Props =
    Props(new PioneerOrderRunner(globalConfig, tradeRoomConfig, exchangeName, exchange, accountData, publicData))
}
class PioneerOrderRunner(globalConfig: GlobalConfig,
                         tradeRoomConfig: TradeRoomConfig,
                         exchangeName: String,
                         exchange: ActorRef,
                         accountData: ExchangeAccountData,
                         publicData: ExchangePublicData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[PioneerOrderRunner])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private val watchSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, 200.millis, self, Watch())
  private val deadline: Instant = Instant.now.plusMillis(globalConfig.internalCommunicationTimeoutDuringInit.duration.toMillis * 3)

  private val pioneerOrder1: AtomicReference[Option[PioneerOrder]] = new AtomicReference(None)
  private val pioneerOrder2: AtomicReference[Option[PioneerOrder]] = new AtomicReference(None)
  private val pioneerOrder3: AtomicReference[Option[PioneerOrder]] = new AtomicReference(None)

  private val order1Validated = WaitingFor()
  private val order2Validated = WaitingFor()
  private val order3Validated = WaitingFor()

  private val order1BalanceUpdateArrived = WaitingFor()
  private val order2BalanceUpdateArrived = WaitingFor()

  private val afterPioneerOrder1BalanceExpected: AtomicReference[Option[Iterable[CryptoValue]]] = new AtomicReference(None)
  private val afterPioneerOrder2BalanceExpected: AtomicReference[Option[Iterable[CryptoValue]]] = new AtomicReference(None)


  def diffMoreThan(a: Double, b: Double, maxDiffRate: Double): Boolean = ((a - b).abs / a.abs) > maxDiffRate

  def validateFilledPioneerOrder(request: OrderRequest, order: Order): Unit = {
    def failed(reasonShort: String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$order")

    if (order.exchange != request.exchange) failed("exchange name mismatch")
    if (order.side != request.tradeSide) failed("trade side mismatch")
    if (order.tradePair != request.tradePair) failed("trade pair mismatch")
    if (order.orderType != OrderType.LIMIT) failed("order type mismatch")
    if (diffMoreThan(order.orderPrice, request.limit, 0.001)) failed("order price mismatch")
    if (diffMoreThan(order.quantity, request.amountBaseAsset, 0.001)) failed("quantity mismatch")

    if (order.orderStatus.isFinal) {
      if (order.orderStatus != OrderStatus.FILLED) failed("order status mismatch")

      if (diffMoreThan(order.cumulativeFilledQuantity, request.amountBaseAsset, 0.003)) failed("cumulative filled quantity mismatch") // in most cases the fee is substracted from the amount we get
      if (request.tradeSide == TradeSide.Buy && (order.priceAverage.isDefined && order.priceAverage.get > request.limit)) failed("price average above limit")
      if (request.tradeSide == TradeSide.Sell && (order.priceAverage.isDefined && order.priceAverage.get < request.limit)) failed("price average below limit")

      val incomingRequested = request.calcIncomingLiquidity
      val incomingReal = order.calcIncomingLiquidity(request.fee)
      val expectedIncomingAsset: Asset = request.tradeSide match {
        case TradeSide.Buy => request.tradePair.baseAsset
        case TradeSide.Sell => request.tradePair.quoteAsset
      }
      if (incomingReal.asset != expectedIncomingAsset) failed("incoming asset mismatch")
      if (incomingReal.amount < incomingRequested.amount && diffMoreThan(incomingReal.amount, incomingRequested.amount, 0.001))
        failed("incoming amount mismatch")

      val expectedOutgoingAsset: Asset = request.tradeSide match {
        case TradeSide.Buy => request.tradePair.quoteAsset
        case TradeSide.Sell => request.tradePair.baseAsset
      }
      val outgoingRequested = request.calcOutgoingLiquidity
      val outgoingReal = order.calcOutgoingLiquidity(request.fee)
      if (outgoingReal.asset != expectedOutgoingAsset) failed("outgoing asset mismatch")
      if (outgoingReal.amount > outgoingRequested.amount && diffMoreThan(outgoingReal.amount, outgoingRequested.amount, 0.001))
        failed("outgoing amount mismatch")

      // We can check the balance only against local ticker, because reference-ticker is not available at this point.
      // The local ticker might not be very up-to-date (like on bitfinex), so we need to be more tolerant regarding the max-diff
      val sumUSDT = OrderBill.aggregateValues(
        OrderBill.calcBalanceSheet(order, Fee(exchangeName, 0.0, 0.0)),
        USDT,
        (_, tradePair) => publicData.ticker.get(tradePair).map(_.priceEstimate))
      if (sumUSDT < -0.03) failed(s"unexpected loss of ${formatDecimal(sumUSDT, 4)} USDT") // more than 3 cent loss is absolutely unacceptable
    }
  }


  def validateCanceledPioneerOrder(request: OrderRequest, order: Order): Unit = {
    def failed(reasonShort: String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$order")

    if (order.exchange != request.exchange) failed("exchange name mismatch")
    if (order.side != request.tradeSide) failed("trade side mismatch")
    if (order.tradePair != request.tradePair) failed("trade pair mismatch")
    if (order.orderType != OrderType.LIMIT) failed("order type mismatch")
    if (diffMoreThan(order.orderPrice, request.limit, 0.001)) failed("order price mismatch")
    if (diffMoreThan(order.quantity, request.amountBaseAsset, 0.001)) failed("quantity mismatch")

    if (order.orderStatus.isFinal) {
      if (order.orderStatus != OrderStatus.CANCELED) failed("order status mismatch")
    }
  }


  def watchOrder(o: PioneerOrder, validationMethod: (OrderRequest, Order) => Unit, arrival: WaitingFor): Unit = {
    if (Instant.now.isAfter(deadline)) {
      throw new RuntimeException("Timeout while waiting for pioneer order to complete")
    }

    accountData.activeOrders.get(o.ref) match {
      case Some(order) if order.orderStatus.isFinal =>
        validationMethod(o.request, order)
        log.info(s"[$exchangeName]  pioneer order ${o.request.shortDesc} succeeded")
        arrival.arrived()

      case Some(order) =>
        log.trace(s"[$exchangeName] pioneer order in progress: $order")
        validationMethod(o.request, order)

      case None => // nop
    }
  }

  def watchBalance(expectedBalance: Iterable[CryptoValue], arrival: WaitingFor): Unit = {
    val SignificantBalanceDeviationInUSDT: Double = 0.50

    var diff: Map[Asset, Double] = expectedBalance.map(e => e.asset -> e.amount).toMap
    for (walletCryptoValue <- accountData.wallet.liquidCryptoValues(USDT, publicData.ticker)) {
      val diffAmount = diff.getOrElse(walletCryptoValue.asset, 0.0) - walletCryptoValue.amount
      diff = diff + (walletCryptoValue.asset -> diffAmount)
    }

    val balanceArrived = !diff.map(e => CryptoValue(e._1, e._2))
      .exists(_.convertTo(USDT, publicData.ticker).amount > SignificantBalanceDeviationInUSDT)
    if (balanceArrived) {
      log.info(s"expected wallet balance arrived")
      arrival.arrived()
    } else {
      log.trace(s"diff between expected minus actual balance is $diff")
    }
  }


  def watchNextEvent(): Unit = {
    if (pioneerOrder1.get().isDefined && !order1Validated.isArrived)
      watchOrder(pioneerOrder1.get().get, (r, o) => validateFilledPioneerOrder(r, o), order1Validated)
    else if (pioneerOrder1.get().isDefined && order1Validated.isArrived && !order1BalanceUpdateArrived.isArrived && afterPioneerOrder1BalanceExpected.get().isDefined)
      watchBalance(afterPioneerOrder1BalanceExpected.get().get, order1BalanceUpdateArrived)
    else if (pioneerOrder2.get().isDefined && !order2Validated.isArrived)
      watchOrder(pioneerOrder2.get().get, (r, o) => validateFilledPioneerOrder(r, o), order2Validated)
    else if (pioneerOrder1.get().isDefined && order2Validated.isArrived && !order2BalanceUpdateArrived.isArrived && afterPioneerOrder2BalanceExpected.get().isDefined)
      watchBalance(afterPioneerOrder2BalanceExpected.get().get, order2BalanceUpdateArrived)
    else if (pioneerOrder3.get().isDefined && !order3Validated.isArrived)
      watchOrder(pioneerOrder3.get().get, (r, o) => validateCanceledPioneerOrder(r, o), order3Validated)
    else
      log.trace(s"nothing to watch: orders: [\n${pioneerOrder1.get()}, \n${pioneerOrder2.get()}, \n${pioneerOrder3.get()}] \n" +
        s"validated: [${order1Validated.isArrived}, ${order2Validated.isArrived}, ${order3Validated.isArrived}]")
  }


  def submitPioneerOrder(tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double, unrealisticGoodlimit: Boolean): PioneerOrder = {
    val realisticLimit = new OrderLimitChooser(
      publicData.orderBook.get(tradePair),
      publicData.ticker(tradePair)
    ).determineRealisticOrderLimit(tradeSide, amountBaseAsset * 5.0, tradeRoomConfig.liquidityManager.txLimitAwayFromEdgeLimit)

    val limit = if (unrealisticGoodlimit) {
      tradeSide match {
        case TradeSide.Buy => realisticLimit * 0.9
        case TradeSide.Sell => realisticLimit * 1.1
      }
    } else {
      realisticLimit
    }

    val orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeName, tradePair, tradeSide, tradeRoomConfig.exchanges(exchangeName).fee,
      amountBaseAsset, limit)

    log.debug(s"[$exchangeName] pioneer order: ${orderRequest.shortDesc}")

    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    val orderRef = Await.result(
      (exchange ? NewLimitOrder(orderRequest)).mapTo[NewOrderAck],
      timeout.duration.plus(1.second))
      .toOrderRef

    PioneerOrder(orderRequest, orderRef)
  }

  def cancelPioneerOrder(o: PioneerOrder): Unit = {
    log.debug(s"[$exchangeName] performing intended cancel of ${o.request.shortDesc}")
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    val cancelOrderResult = Await.result(
      (exchange ? CancelOrder(o.ref.tradePair, o.ref.externalOrderId)).mapTo[CancelOrderResult],
      timeout.duration.plus(1.second))
    if (!cancelOrderResult.success) {
      throw new RuntimeException(s"Intended cancel of PioneerOrder ${
        o.request.shortDesc
      } failed: $cancelOrderResult")
    }
  }

  def submitFirstPioneerOrder(): Unit = {
    val balanceBeforeOrder: Iterable[CryptoValue] = accountData.wallet.liquidCryptoValues(USDT, publicData.ticker)
    val amountBitcoin = CryptoValue(USDT, tradeRoomConfig.pioneerOrderValueUSDT).convertTo(Bitcoin, publicData.ticker).amount
    pioneerOrder1.set(Some(submitPioneerOrder(TradePair(Bitcoin, USDT), TradeSide.Buy, amountBitcoin, unrealisticGoodlimit = false)))
    val expectedBalanceDiff: Seq[CryptoValue] = OrderBill.calcBalanceSheet(pioneerOrder1.get().get.request).map(e => CryptoValue(e.asset, e.amount))
    afterPioneerOrder1BalanceExpected.set(Some(Util.applyBalanceDiff(balanceBeforeOrder, expectedBalanceDiff)))
  }

  def submitSecondPioneerOrder(): Unit = {
    val balanceBeforeOrder = accountData.wallet.liquidCryptoValues(USDT, publicData.ticker)
    val amountBitcoin = CryptoValue(USDT, tradeRoomConfig.pioneerOrderValueUSDT).convertTo(Bitcoin, publicData.ticker).amount
    pioneerOrder2.set(Some(submitPioneerOrder(TradePair(Bitcoin, USDT), TradeSide.Sell, amountBitcoin, unrealisticGoodlimit = false)))
    val expectedBalanceDiff: Seq[CryptoValue] = OrderBill.calcBalanceSheet(pioneerOrder2.get().get.request).map(e => CryptoValue(e.asset, e.amount))
    afterPioneerOrder2BalanceExpected.set(Some(Util.applyBalanceDiff(balanceBeforeOrder, expectedBalanceDiff)))
  }

  def submitBuyToCancelPioneerOrder(): Unit = {
    val amountBitcoin = CryptoValue(USDT, tradeRoomConfig.pioneerOrderValueUSDT).convertTo(Bitcoin, publicData.ticker).amount
    pioneerOrder3.set(Some(submitPioneerOrder(TradePair(Bitcoin, USDT), TradeSide.Buy, amountBitcoin, unrealisticGoodlimit = true)))
    Thread.sleep(500)
    cancelPioneerOrder(pioneerOrder3.get().get)
  }

  override def preStart(): Unit = {
    log.info(s"running pioneer order for $exchangeName")

    val maxWaitTime = globalConfig.internalCommunicationTimeoutDuringInit.duration
    val balanceUpdateMaxWaitTime: FiniteDuration = 5.seconds
    val InitSequence = new InitSequence(log, s"$exchangeName  PioneerOrderRunner", List(
      InitStep("Submit pioneer order 1 (buy Bitcoin from USDT)", () => submitFirstPioneerOrder()),
      InitStep("Waiting until Pioneer order 1 is validated", () => order1Validated.await(maxWaitTime)),
      InitStep("Waiting until wallet balance reflects update from order 1", () => order1BalanceUpdateArrived.await(balanceUpdateMaxWaitTime)),
      InitStep("Submit pioneer order 2 (sell Bitcoin to USDT)", () => submitSecondPioneerOrder()),
      InitStep("Waiting until Pioneer order 2 is validated", () => order2Validated.await(maxWaitTime)),
      InitStep("Waiting until wallet balance reflects update from order 2", () => order2BalanceUpdateArrived.await(balanceUpdateMaxWaitTime)),
      InitStep("Submit pioneer order 3 (cancel another buy Bitcoin order)", () => submitBuyToCancelPioneerOrder()),
      InitStep("Waiting until Pioneer order 3 is validated", () => order3Validated.await(maxWaitTime))
    ))

    Future(InitSequence.run()).onComplete {
      case Success(_) =>
        log.info(s"[$exchangeName] PioneerOrderRunner successful completed")
        exchange ! PioneerOrderSucceeded()
        stop()
      case Failure(e) =>
        log.error(s"[$exchangeName] PioneerOrderRunner failed", e)
        exchange ! PioneerOrderFailed(e)
        stop()
    }
  }

  def stop(): Unit = {
    watchSchedule.cancel()
    self ! PoisonPill
  } // TODO coordinated shutdown

  override def receive: Receive = {
    case Watch() => watchNextEvent()
    case Failure(e) =>
      log.error(s"PioneerOrderRunner failed", e)
      exchange ! PioneerOrderFailed(e)
      stop()
  }
}
