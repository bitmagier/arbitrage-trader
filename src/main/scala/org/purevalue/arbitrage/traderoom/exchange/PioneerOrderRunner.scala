package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrder, PioneerOrderFailed, PioneerOrderSucceeded, Watch}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.util.{InitSequence, InitStep, Util, WaitingFor}
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main}

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
            exchange: ActorRef): Props =
    Props(new PioneerOrderRunner(config, exchangeConfig, exchange))
}
class PioneerOrderRunner(config: Config,
                         exchangeConfig: ExchangeConfig,
                         exchange: ActorRef) extends Actor with ActorLogging {
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private val watchSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, 200.millis, self, Watch())
  private val deadline: Instant = Instant.now.plusMillis(config.global.internalCommunicationTimeoutDuringInit.duration.toMillis * 3)

  private val exchangeName = exchangeConfig.name
  private val primaryReserveAsset = exchangeConfig.reserveAssets.head
  private val secondaryReserveAsset = exchangeConfig.reserveAssets(1)

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


  def getPriceEstimate(pair: TradePair): Double = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    Await.result(
      (exchange ? GetPriceEstimate(pair)).mapTo[Double],
      timeout.duration.plus(500.millis)
    )
  }

  def diffMoreThan(a: Double, b: Double, maxDiffRate: Double): Boolean = ((a - b).abs / a.abs) > maxDiffRate

  def validateFilledPioneerOrder(request: OrderRequest, order: Order): Unit = {
    def failed(reasonShort: String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$order")

    if (order.exchange != request.exchange) failed("exchange name mismatch")
    if (order.side != request.side) failed("trade side mismatch")
    if (order.pair != request.pair) failed("trade pair mismatch")
    if (order.orderType != OrderType.LIMIT) failed("order type mismatch")
    if (diffMoreThan(order.quantity, request.amountBaseAsset, MaxAmountDiff)) failed("quantity mismatch")

    if (order.orderStatus.isFinal) {
      if (order.price.isEmpty) failed("order price not set")
      if (diffMoreThan(order.price.get, request.limit, MaxPriceDiff)) failed("order limit/price mismatch")
      if (order.orderStatus != OrderStatus.FILLED) failed("order status mismatch")

      if (order.cumulativeFilledQuantity.isEmpty) failed("cumulativeFilledQuantity not set")
      // This check is not applicable with the small acceptable diff value on bitfinex, because sometimes the reported fill is a lot less than the requested amount.
      // Example: amountBaseAsset:0.00175778 versus cumulativeFilledQty: Some(0.0015133)
      // OrderRequest(51c18f89-71b9-4a46-b367-4e4a242c251d, orderBundleId:None, bitfinex, BTC:USDT, Buy, 0.0015, amountBaseAsset:0.00175778, limit:11386.2768),
      // Order(52458298624,bitfinex,BTC:USDT,Buy,LIMIT,Some(11386.0),None,2020-10-10T08:39:21.581Z,0.00175778,FILLED,Some(0.0015133),Some(11383.443661891704),2020-10-10T08:39:21.587Z)
      val maxAmountDiff = if (exchangeConfig.name == "bitfinex") 0.2 else MaxAmountDiff
      if (diffMoreThan(order.cumulativeFilledQuantity.get, request.amountBaseAsset, maxAmountDiff)) failed("cumulative filled quantity mismatch") // in most cases the fee is substracted from the amount we get

      val PriceAverageRoundingToleranceRate = 0.00000001
      if (request.side == TradeSide.Buy && order.priceAverage.isDefined &&
        order.priceAverage.get * (1.0 - PriceAverageRoundingToleranceRate) > order.price.get) failed("price average above price/limit")
      if (request.side == TradeSide.Sell && order.priceAverage.isDefined &&
        order.priceAverage.get * (1.0 + PriceAverageRoundingToleranceRate) < order.price.get) failed("price average below price/limit")

      val incomingRequested = request.calcIncomingLiquidity
      val incomingReal = order.calcIncomingLiquidity(request.feeRate)
      val expectedIncomingAsset: Asset = request.side match {
        case TradeSide.Buy => request.pair.baseAsset
        case TradeSide.Sell => request.pair.quoteAsset
      }
      if (incomingReal.asset != expectedIncomingAsset) failed("incoming asset mismatch")
      if (incomingReal.amount < incomingRequested.amount && diffMoreThan(incomingReal.amount, incomingRequested.amount, MaxAmountDiff))
        failed("incoming amount mismatch")

      val expectedOutgoingAsset: Asset = request.side match {
        case TradeSide.Buy => request.pair.quoteAsset
        case TradeSide.Sell => request.pair.baseAsset
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
        (_, tradePair) => Some(getPriceEstimate(tradePair)))
      val maxAcceptableLoss = 0.03 + (request.feeRate * config.tradeRoom.pioneerOrderValueUSD)
      if (sumUSD < -maxAcceptableLoss) failed(s"unexpected loss of ${formatDecimal(sumUSD, 4)} USD") // more than 3 cent + fee loss is absolutely unacceptable
    }
  }


  def validateCanceledPioneerOrder(request: OrderRequest, order: Order): Unit = {
    def failed(reasonShort: String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$order")

    if (order.exchange != request.exchange) failed("exchange name mismatch")
    if (order.side != request.side) failed("trade side mismatch")
    if (order.pair != request.pair) failed("trade pair mismatch")
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

    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    (exchange ? GetActiveOrders()).mapTo[Map[OrderRef, Order]].foreach { activeOrders =>

      activeOrders.get(o.ref) match {
        case Some(order) if order.orderStatus.isFinal =>
          validationMethod(o.request, order)
          log.info(s"[$exchangeName]  pioneer order ${o.request.shortDesc} successfully validated")
          arrival.arrived()
          exchange ! RemoveActiveOrder(o.ref)

        case Some(order) =>
          log.debug(s"[$exchangeName] pioneer order in progress: $order")
          validationMethod(o.request, order)

        case None => // nop

      }
    }
  }

  def walletLiquidCryptoValues(): Future[Iterable[CryptoValue]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    (exchange ? GetWalletLiquidCrypto()).mapTo[Iterable[CryptoValue]]
  }

  def convert(value: CryptoValue, targetAsset: Asset)(implicit timeout: Timeout): Future[CryptoValue] = {
    (exchange ? ConvertValue(value, targetAsset)).mapTo[CryptoValue]
  }

  def watchBalance(expectedBalance: Iterable[CryptoValue], arrival: WaitingFor): Unit = {
    val SignificantBalanceDeviationInUSD: Double = 0.50

    var balanceDiff: Map[Asset, Double] = expectedBalance.map(e => e.asset -> e.amount).toMap

    walletLiquidCryptoValues().foreach { liquidCryptoValues =>
      for (walletCryptoValue <- liquidCryptoValues) {
        val diffAmount = balanceDiff.getOrElse(walletCryptoValue.asset, 0.0) - walletCryptoValue.amount
        balanceDiff = balanceDiff + (walletCryptoValue.asset -> diffAmount)
      }

      implicit val timeout: Timeout = config.global.internalCommunicationTimeout
      Future.sequence(
        balanceDiff
          .map(e => CryptoValue(e._1, e._2))
          .map(e => convert(e, exchangeConfig.usdEquivalentCoin))
      ).foreach { balanceDiffInUSD =>
        if (!balanceDiffInUSD.exists(_.amount > SignificantBalanceDeviationInUSD)) {
          log.debug(s"[$exchangeName] expected wallet balance arrived")
          arrival.arrived()
        } else {
          log.debug(s"diff between expected minus actual balance is $balanceDiffInUSD")
        }
      }
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
        log.debug(s"nothing to watch: orders: [\n${pioneerOrder1.get()}, \n${pioneerOrder2.get()}, \n${pioneerOrder3.get()}] \n" +
          s"validated: [${order1Validated.isArrived}, ${order2Validated.isArrived}, ${order3Validated.isArrived}]")
    } catch {
      case e: Throwable =>
        log.debug(s"[$exchangeName] PioneerOrderRunner failed", e)
        exchange ! PioneerOrderFailed(e)
        stop()
    }
  }


  def submitPioneerOrder(pair: TradePair, side: TradeSide, amountBaseAsset: Double, unrealisticGoodlimit: Boolean): Future[PioneerOrder] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout

    (exchange ? DetermineRealisticLimit(pair, side, amountBaseAsset * 5.0, config.liquidityManager.setTxLimitAwayFromEdgeLimit))
      .mapTo[Double]
      .flatMap {
        realisticLimit =>
          val limit = if (unrealisticGoodlimit) {
            side match {
              case TradeSide.Buy => realisticLimit * 0.9
              case TradeSide.Sell => realisticLimit * 1.1
            }
          } else {
            realisticLimit
          }

          val orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeName, pair, side, exchangeConfig.feeRate, amountBaseAsset, limit)

          log.debug(s"[$exchangeName] pioneer order: ${orderRequest.shortDesc}")

          (exchange ? NewLimitOrder(orderRequest)).mapTo[NewOrderAck]
            .map(_.toOrderRef)
            .map(PioneerOrder(orderRequest, _))
      }
  }

  def cancelPioneerOrder(o: PioneerOrder): Unit = {
    log.debug(s"[$exchangeName] performing intended cancel of ${o.request.shortDesc}")
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit

    val cancelOrderResult = Await.result(
      (exchange ? CancelOrder(o.ref)).mapTo[CancelOrderResult],
      timeout.duration.plus(1.second))

    if (!cancelOrderResult.success) {
      throw new RuntimeException(s"Intended cancel of PioneerOrder ${o.request.shortDesc} failed: $cancelOrderResult")
    }
  }

  def submitFirstPioneerOrder(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout

    val tradePair = TradePair(secondaryReserveAsset, primaryReserveAsset)
    val (balanceBeforeOrder, pioneerOrder) = Await.result(
      for {
        balanceBeforeOrder <- walletLiquidCryptoValues()
        amount <- convert(
          CryptoValue(exchangeConfig.usdEquivalentCoin, config.tradeRoom.pioneerOrderValueUSD),
          tradePair.baseAsset)
          .map(_.amount)
        pioneerOrder <- submitPioneerOrder(tradePair, TradeSide.Buy, amount, unrealisticGoodlimit = false)
      } yield (balanceBeforeOrder, pioneerOrder),
      timeout.duration.plus(500.millis)
    )

    pioneerOrder1.set(Some(pioneerOrder))

    val expectedBalanceDiff: Seq[CryptoValue] =
      OrderBill.calcBalanceSheet(pioneerOrder1.get().get.request)
        .map(e => CryptoValue(e.asset, e.amount))
    afterPioneerOrder1BalanceExpected.set(Some(Util.applyBalanceDiff(balanceBeforeOrder, expectedBalanceDiff)))
  }

  def submitSecondPioneerOrder(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val tradePair = TradePair(secondaryReserveAsset, primaryReserveAsset)
    val (balanceBeforeOrder, pioneerOrder) = Await.result(
      for {
        balanceBeforeOrder <- walletLiquidCryptoValues()
        amount <- convert(
          CryptoValue(exchangeConfig.usdEquivalentCoin, config.tradeRoom.pioneerOrderValueUSD),
          tradePair.baseAsset)
          .map(_.amount)
        pioneerOrder <- submitPioneerOrder(tradePair, TradeSide.Sell, amount, unrealisticGoodlimit = false)
      } yield (balanceBeforeOrder, pioneerOrder),
      timeout.duration.plus(500.millis)
    )

    pioneerOrder2.set(Some(pioneerOrder))

    val expectedBalanceDiff: Seq[CryptoValue] =
      OrderBill.calcBalanceSheet(pioneerOrder2.get().get.request)
        .map(e => CryptoValue(e.asset, e.amount))
    afterPioneerOrder2BalanceExpected.set(Some(Util.applyBalanceDiff(balanceBeforeOrder, expectedBalanceDiff)))
  }

  def submitBuyToCancelPioneerOrder(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val pioneerOrder = Await.result(
      for {
        amountToBuy <- convert(
          CryptoValue(exchangeConfig.usdEquivalentCoin, config.tradeRoom.pioneerOrderValueUSD),
          secondaryReserveAsset)
          .map(_.amount)
        pioneerOrder <- submitPioneerOrder(
          TradePair(secondaryReserveAsset, exchangeConfig.usdEquivalentCoin),
          TradeSide.Buy,
          amountToBuy,
          unrealisticGoodlimit = true)
      } yield pioneerOrder,
      timeout.duration.plus(500.millis)
    )
    pioneerOrder3.set(Some(pioneerOrder))

    Thread.sleep(500)
    cancelPioneerOrder(pioneerOrder3.get().get)
  }

  override def preStart(): Unit = {
    log.info(s"running pioneer order for $exchangeName")

    val maxWaitTime = config.global.internalCommunicationTimeoutDuringInit.duration
    val InitSequence = new InitSequence(log, s"$exchangeName  PioneerOrderRunner",
      List(
        InitStep(s"Submit pioneer order 1 (buy ${secondaryReserveAsset.officialSymbol} from ${primaryReserveAsset.officialSymbol})", () => submitFirstPioneerOrder()),
        InitStep("Waiting until Pioneer order 1 is validated", () => order1Validated.await(maxWaitTime)),
        InitStep("Waiting until wallet balance reflects update from order 1", () => order1BalanceUpdateArrived.await(maxWaitTime)),
        InitStep(s"Submit pioneer order 2 (sell ${secondaryReserveAsset.officialSymbol} to ${primaryReserveAsset.officialSymbol})", () => submitSecondPioneerOrder()),
        InitStep("Waiting until Pioneer order 2 is validated", () => order2Validated.await(maxWaitTime)),
        InitStep("Waiting until wallet balance reflects update from order 2", () => order2BalanceUpdateArrived.await(maxWaitTime)),
        InitStep("Submit pioneer order 3 and directly cancel that order", () => submitBuyToCancelPioneerOrder()),
        InitStep("Waiting until Pioneer order 3 is validated", () => order3Validated.await(maxWaitTime))
      ))

    Future(InitSequence.run()).onComplete {
      case Success(_) =>
        log.info(s"[$exchangeName] PioneerOrderRunner successful completed")
        exchange ! PioneerOrderSucceeded()
        stop()
      case Failure(e) =>
        log.error(e, s"[$exchangeName] PioneerOrderRunner failed")
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
    case Failure(e) => // TODO
      log.error(e, s"PioneerOrderRunner failed")
      exchange ! PioneerOrderFailed(e)
      stop()
  }
}
