package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, CancelOrderResult, NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.adapter.{ExchangeAccountData, ExchangePublicData}
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrder, PioneerOrderFailed, PioneerOrderSucceeded, Watch}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.util.{InitSequence, InitStep, Util, WaitingFor}
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


// run some validated pioneer orders before the exchange gets available for further orders
object PioneerOrderRunner {
  case class Watch()
  case class PioneerOrderSucceeded()
  case class PioneerOrderFailed(exception: Throwable)

  case class PioneerOrder(request: OrderRequest, ref: OrderRef)

  def props(config: Config,
            exchangeConfig: ExchangeConfig,
            exchange: ActorRef,
            accountData: ExchangeAccountData,
            publicData: ExchangePublicData): Props =
    Props(new PioneerOrderRunner(config, exchangeConfig, exchange, accountData, publicData))
}
class PioneerOrderRunner(config: Config,
                         exchangeConfig: ExchangeConfig,
                         exchange: ActorRef,
                         accountData: ExchangeAccountData,
                         publicData: ExchangePublicData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[PioneerOrderRunner])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private val watchSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, 200.millis, self, Watch())
  private val deadline: Instant = Instant.now.plusMillis(config.global.internalCommunicationTimeoutDuringInit.duration.toMillis * 3)

  private val ExchangeName = exchangeConfig.name
  private val PrimaryReserveAsset = exchangeConfig.reserveAssets.head
  private val SecondaryReserveAsset = exchangeConfig.reserveAssets(1)

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

  private val MaxPriceDiff = 0.001
  private val MaxAmountDiff = 0.003


  def diffMoreThan(a: Double, b: Double, maxDiffRate: Double): Boolean = ((a - b).abs / a.abs) > maxDiffRate

  def validateFilledPioneerOrder(request: OrderRequest, order: Order): Unit = {
    def failed(reasonShort: String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$order")

    if (order.exchange != request.exchange) failed("exchange name mismatch")
    if (order.side != request.tradeSide) failed("trade side mismatch")
    if (order.tradePair != request.tradePair) failed("trade pair mismatch")
    if (order.orderType != OrderType.LIMIT) failed("order type mismatch")
    if (diffMoreThan(order.quantity, request.amountBaseAsset, MaxAmountDiff)) failed("quantity mismatch")

    if (order.orderStatus.isFinal) {
      if (order.price.isEmpty) failed("order price not set")
      if (diffMoreThan(order.price.get, request.limit, MaxPriceDiff)) failed("order limit/price mismatch")
      if (order.orderStatus != OrderStatus.FILLED) failed("order status mismatch")

      if (order.cumulativeFilledQuantity.isEmpty) failed("cumulativeFilledQuantity not set")
      if (diffMoreThan(order.cumulativeFilledQuantity.get, request.amountBaseAsset, MaxAmountDiff)) failed("cumulative filled quantity mismatch") // in most cases the fee is substracted from the amount we get
      val PriceAverageRoundingToleranceRate = 0.00000001
      if (request.tradeSide == TradeSide.Buy && order.priceAverage.isDefined &&
        order.priceAverage.get * (1.0 - PriceAverageRoundingToleranceRate) > order.price.get) failed("price average above price/limit")
      if (request.tradeSide == TradeSide.Sell && order.priceAverage.isDefined &&
        order.priceAverage.get * (1.0 + PriceAverageRoundingToleranceRate) < order.price.get) failed("price average below price/limit")

      val incomingRequested = request.calcIncomingLiquidity
      val incomingReal = order.calcIncomingLiquidity(request.feeRate)
      val expectedIncomingAsset: Asset = request.tradeSide match {
        case TradeSide.Buy => request.tradePair.baseAsset
        case TradeSide.Sell => request.tradePair.quoteAsset
      }
      if (incomingReal.asset != expectedIncomingAsset) failed("incoming asset mismatch")
      if (incomingReal.amount < incomingRequested.amount && diffMoreThan(incomingReal.amount, incomingRequested.amount, MaxAmountDiff))
        failed("incoming amount mismatch")

      val expectedOutgoingAsset: Asset = request.tradeSide match {
        case TradeSide.Buy => request.tradePair.quoteAsset
        case TradeSide.Sell => request.tradePair.baseAsset
      }
      val outgoingRequested = request.calcOutgoingLiquidity
      val outgoingReal = order.calcOutgoingLiquidity(request.feeRate)
      if (outgoingReal.asset != expectedOutgoingAsset) failed("outgoing asset mismatch")
      if (outgoingReal.amount > outgoingRequested.amount && diffMoreThan(outgoingReal.amount, outgoingRequested.amount, MaxAmountDiff))
        failed("outgoing amount mismatch")

      // We can check the balance only against local ticker, because reference-ticker is not available at this point.
      // The local ticker might not be very up-to-date (like on bitfinex), so we need to be more tolerant regarding the max-diff
      val sumUSD = OrderBill.aggregateValues(
        OrderBill.calcBalanceSheet(order, request.feeRate),
        exchangeConfig.usdEquivalentCoin,
        (_, tradePair) => publicData.ticker.get(tradePair).map(_.priceEstimate))
      val maxAcceptableLoss = 0.03 + (request.feeRate * config.tradeRoom.pioneerOrderValueUSD)
      if (sumUSD < -maxAcceptableLoss) failed(s"unexpected loss of ${formatDecimal(sumUSD, 4)} USD") // more than 3 cent + fee loss is absolutely unacceptable
    }
  }


  def validateCanceledPioneerOrder(request: OrderRequest, order: Order): Unit = {
    def failed(reasonShort: String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$order")

    if (order.exchange != request.exchange) failed("exchange name mismatch")
    if (order.side != request.tradeSide) failed("trade side mismatch")
    if (order.tradePair != request.tradePair) failed("trade pair mismatch")
    if (order.orderType != OrderType.LIMIT) failed("order type mismatch")
    if (order.price.isEmpty) failed("orderPrice not set")
    if (diffMoreThan(order.price.get, request.limit, MaxPriceDiff)) failed("order price mismatch")
    if (diffMoreThan(order.quantity, request.amountBaseAsset, MaxAmountDiff)) failed("quantity mismatch")

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
        log.info(s"[$ExchangeName]  pioneer order ${o.request.shortDesc} successfully validated")
        arrival.arrived()
        accountData.activeOrders.remove(o.ref)

      case Some(order) =>
        log.trace(s"[$ExchangeName] pioneer order in progress: $order")
        validationMethod(o.request, order)

      case None => // nop
    }
  }

  def watchBalance(expectedBalance: Iterable[CryptoValue], arrival: WaitingFor): Unit = {
    val SignificantBalanceDeviationInUSD: Double = 0.50

    var diff: Map[Asset, Double] = expectedBalance.map(e => e.asset -> e.amount).toMap
    for (walletCryptoValue <- accountData.wallet.liquidCryptoValues(exchangeConfig.usdEquivalentCoin, publicData.ticker)) {
      val diffAmount = diff.getOrElse(walletCryptoValue.asset, 0.0) - walletCryptoValue.amount
      diff = diff + (walletCryptoValue.asset -> diffAmount)
    }

    val balanceArrived = !diff.map(e => CryptoValue(e._1, e._2))
      .exists(_.convertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker).amount > SignificantBalanceDeviationInUSD)
    if (balanceArrived) {
      log.info(s"expected wallet balance arrived")
      arrival.arrived()
    } else {
      log.trace(s"diff between expected minus actual balance is $diff")
    }
  }


  def watchNextEvent(): Unit = {
    try {
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
    } catch {
      case e: Throwable =>
        log.debug(s"[$ExchangeName] PioneerOrderRunner failed", e)
        exchange ! PioneerOrderFailed(e)
        stop()
    }
  }


  def submitPioneerOrder(tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double, unrealisticGoodlimit: Boolean): PioneerOrder = {
    val realisticLimit = new OrderLimitChooser(
      publicData.orderBook.get(tradePair),
      publicData.ticker(tradePair)
    ).determineRealisticOrderLimit(tradeSide, amountBaseAsset * 5.0, config.liquidityManager.txLimitAwayFromEdgeLimit).get

    val limit = if (unrealisticGoodlimit) {
      tradeSide match {
        case TradeSide.Buy => realisticLimit * 0.9
        case TradeSide.Sell => realisticLimit * 1.1
      }
    } else {
      realisticLimit
    }

    val orderRequest = OrderRequest(UUID.randomUUID(), None, ExchangeName, tradePair, tradeSide, exchangeConfig.feeRate,
      amountBaseAsset, limit)

    log.debug(s"[$ExchangeName] pioneer order: ${orderRequest.shortDesc}")

    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    val orderRef = Await.result(
      (exchange ? NewLimitOrder(orderRequest)).mapTo[NewOrderAck],
      timeout.duration.plus(1.second))
      .toOrderRef

    PioneerOrder(orderRequest, orderRef)
  }

  def cancelPioneerOrder(o: PioneerOrder): Unit = {
    log.debug(s"[$ExchangeName] performing intended cancel of ${o.request.shortDesc}")
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit

    val cancelOrderResult = Await.result(
      (exchange ? CancelOrder(o.ref)).mapTo[CancelOrderResult],
      timeout.duration.plus(1.second))

    if (!cancelOrderResult.success) {
      throw new RuntimeException(s"Intended cancel of PioneerOrder ${o.request.shortDesc} failed: $cancelOrderResult")
    }
  }

  def submitFirstPioneerOrder(): Unit = {
    val balanceBeforeOrder: Iterable[CryptoValue] = accountData.wallet.liquidCryptoValues(exchangeConfig.usdEquivalentCoin, publicData.ticker)
    val tradePair = TradePair(SecondaryReserveAsset, PrimaryReserveAsset)
    val amount = CryptoValue(exchangeConfig.usdEquivalentCoin, config.tradeRoom.pioneerOrderValueUSD)
      .convertTo(tradePair.baseAsset, publicData.ticker).amount

    pioneerOrder1.set(Some(
      submitPioneerOrder(tradePair, TradeSide.Buy, amount, unrealisticGoodlimit = false)))

    val expectedBalanceDiff: Seq[CryptoValue] =
      OrderBill.calcBalanceSheet(pioneerOrder1.get().get.request)
        .map(e => CryptoValue(e.asset, e.amount))

    afterPioneerOrder1BalanceExpected.set(Some(Util.applyBalanceDiff(balanceBeforeOrder, expectedBalanceDiff)))
  }

  def submitSecondPioneerOrder(): Unit = {
    val balanceBeforeOrder = accountData.wallet.liquidCryptoValues(exchangeConfig.usdEquivalentCoin, publicData.ticker)
    val tradePair = TradePair(SecondaryReserveAsset, PrimaryReserveAsset)
    val amount = CryptoValue(exchangeConfig.usdEquivalentCoin, config.tradeRoom.pioneerOrderValueUSD)
      .convertTo(tradePair.baseAsset, publicData.ticker).amount

    pioneerOrder2.set(Some(
      submitPioneerOrder(tradePair, TradeSide.Sell, amount, unrealisticGoodlimit = false)))

    val expectedBalanceDiff: Seq[CryptoValue] =
      OrderBill.calcBalanceSheet(pioneerOrder2.get().get.request)
        .map(e => CryptoValue(e.asset, e.amount))

    afterPioneerOrder2BalanceExpected.set(Some(Util.applyBalanceDiff(balanceBeforeOrder, expectedBalanceDiff)))
  }

  def submitBuyToCancelPioneerOrder(): Unit = {
    val amountToBuy = CryptoValue(exchangeConfig.usdEquivalentCoin, config.tradeRoom.pioneerOrderValueUSD)
      .convertTo(SecondaryReserveAsset, publicData.ticker).amount

    pioneerOrder3.set(Some(
      submitPioneerOrder(
        TradePair(SecondaryReserveAsset, exchangeConfig.usdEquivalentCoin),
        TradeSide.Buy,
        amountToBuy,
        unrealisticGoodlimit = true)))

    Thread.sleep(500)
    cancelPioneerOrder(pioneerOrder3.get().get)
  }

  override def preStart(): Unit = {
    log.info(s"running pioneer order for $ExchangeName")

    val maxWaitTime = config.global.internalCommunicationTimeoutDuringInit.duration
    val InitSequence = new InitSequence(log, s"$ExchangeName  PioneerOrderRunner",
      List(
        InitStep(s"Submit pioneer order 1 (buy ${SecondaryReserveAsset.officialSymbol} from ${PrimaryReserveAsset.officialSymbol})", () => submitFirstPioneerOrder()),
        InitStep("Waiting until Pioneer order 1 is validated", () => order1Validated.await(maxWaitTime)),
        InitStep("Waiting until wallet balance reflects update from order 1", () => order1BalanceUpdateArrived.await(maxWaitTime)),
        InitStep(s"Submit pioneer order 2 (sell ${SecondaryReserveAsset.officialSymbol} to ${PrimaryReserveAsset.officialSymbol})", () => submitSecondPioneerOrder()),
        InitStep("Waiting until Pioneer order 2 is validated", () => order2Validated.await(maxWaitTime)),
        InitStep("Waiting until wallet balance reflects update from order 2", () => order2BalanceUpdateArrived.await(maxWaitTime)),
        InitStep("Submit pioneer order 3 and directly cancel that order", () => submitBuyToCancelPioneerOrder()),
        InitStep("Waiting until Pioneer order 3 is validated", () => order3Validated.await(maxWaitTime))
      ))

    Future(InitSequence.run()).onComplete {
      case Success(_) =>
        log.info(s"[$ExchangeName] PioneerOrderRunner successful completed")
        exchange ! PioneerOrderSucceeded()
        stop()
      case Failure(e) =>
        log.error(s"[$ExchangeName] PioneerOrderRunner failed", e)
        exchange ! PioneerOrderFailed(e)
        stop()
    }
  }

  def stop(): Unit = {
    watchSchedule.cancel()
    self ! PoisonPill
  }

  override def receive: Receive = {
    case Watch() => watchNextEvent()
    case Failure(e) =>
      log.error(s"PioneerOrderRunner failed", e)
      exchange ! PioneerOrderFailed(e)
      stop()
  }
}
