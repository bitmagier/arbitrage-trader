package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.adapter.{Fee, Ticker}
import org.purevalue.arbitrage.traderoom.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrderFailed, PioneerOrderSucceeded, Watch}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{GlobalConfig, Main, TradeRoomConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}


// run a validated pioneer order before the exchange gets available for further orders
// we buy BTC from USDT (configured amount)
// TODO add another order, to check if cancel-ordewr works
object PioneerOrderRunner {
  case class Watch()
  case class PioneerOrderSucceeded()
  case class PioneerOrderFailed(exception: Exception)

  def props(globalConfig: GlobalConfig,
            tradeRoomConfig: TradeRoomConfig,
            exchangeName: String,
            exchange: ActorRef,
            tickers: collection.Map[TradePair, Ticker],
            activeOrderLookup: OrderRef => Option[Order]
           ): Props =
    Props(new PioneerOrderRunner(globalConfig, tradeRoomConfig, exchangeName, exchange, tickers, activeOrderLookup))
}
class PioneerOrderRunner(globalConfig: GlobalConfig,
                         tradeRoomConfig: TradeRoomConfig,
                         exchangeName: String,
                         exchange: ActorRef,
                         tickers: collection.Map[TradePair, Ticker],
                         activeOrderLookup: OrderRef => Option[Order]
                        ) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[PioneerOrderRunner])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val watchSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, 500.millis, self, Watch())
  val deadline: Instant = Instant.now.plusMillis(globalConfig.internalCommunicationTimeoutDuringInit.duration.toMillis)
  var orderRequest: OrderRequest = _
  var orderRef: OrderRef = _

  def validateFinishedPioneerOrder(request: OrderRequest, finishedOrder: Order): Unit = {
    def failed(reasonShort:String) = throw new RuntimeException(s"Pioneer order validation failed. reason: '$reasonShort'! \n$request, \n$finishedOrder")

    def diffMoreThan(a: Double, b: Double, maxDiffRate: Double): Boolean = ((a - b).abs / a.abs) > maxDiffRate

    if (finishedOrder.exchange != request.exchange) failed("exchange name mismatch")
    if (finishedOrder.side != request.tradeSide) failed("trade side mismatch")
    if (finishedOrder.tradePair != request.tradePair) failed("trade pair mismatch")
    if (finishedOrder.orderType != OrderType.LIMIT) failed("order type mismatch")
    if (diffMoreThan(finishedOrder.orderPrice, request.limit, 0.0001)) failed("order price mismatch")

    if (finishedOrder.orderStatus != OrderStatus.FILLED) failed("order status mismatch")
    if (diffMoreThan(finishedOrder.quantity, request.amountBaseAsset, 0.0001)) failed("quantity mismatch")
    if (diffMoreThan(finishedOrder.cumulativeFilledQuantity, request.amountBaseAsset, 0.0001)) failed("cumulative filled quantity mismatch")
    if (request.tradeSide == TradeSide.Buy && (finishedOrder.priceAverage > request.limit)) failed("price average above limit")
    if (request.tradeSide == TradeSide.Sell && (finishedOrder.priceAverage < request.limit)) failed("price average below limit")

    val incomingRequested = request.calcIncomingLiquidity
    val incomingReal = finishedOrder.calcIncomingLiquidity(request.fee)
    if (incomingReal.asset != Bitcoin) failed("incoming asset mismatch")
    if (incomingReal.amount < incomingRequested.amount && diffMoreThan(incomingReal.amount, incomingRequested.amount, 0.0001))
      failed("incoming amount mismatch")

    val outgoingRequested = request.calcOutgoingLiquidity
    val outgoingReal = finishedOrder.calcOutgoingLiquidity(request.fee)
    if (outgoingReal.asset != USDT) failed("outgoing asset mismatch")
    if (outgoingReal.amount > outgoingRequested.amount && diffMoreThan(outgoingReal.amount, outgoingRequested.amount, 0.0001))
      failed("outgoing amount mismatch")

    val sumUSDT = OrderBill.aggregateValues(
      OrderBill.calcBalanceSheet(finishedOrder, Fee(exchangeName, 0.0, 0.0)),
      USDT,
      (_, tradePair) => tickers.get(tradePair).map(_.priceEstimate))
    if (sumUSDT < -0.01) failed(s"unexpected loss of ${formatDecimal(sumUSDT, 4)} USDT") // more than 1 cent loss is absolutely unacceptable
  }

  def watchPioneerOrder(): Unit = {
    if (Instant.now.isAfter(deadline)) {
      val e = new RuntimeException("Timeout while waiting for pioneer order to complete")
      log.debug(e.getMessage)
      exchange ! PioneerOrderFailed(e)
      stop()
    }

    activeOrderLookup(orderRef) match {
      case Some(order) if !order.orderStatus.isFinal =>
        log.trace(s"[$exchangeName] pioneer order in progress")

      case Some(order) =>
        try {
          validateFinishedPioneerOrder(orderRequest, order)
          log.info(s"pioneer order for $exchangeName succeeded")
          exchange ! PioneerOrderSucceeded()
        } catch {
          case e: Exception => exchange ! PioneerOrderFailed(e)
        }
        stop()

      case None => // nop
    }
  }

  override def preStart(): Unit = {
    log.info(s"running pioneer order for $exchangeName")
    val tradePair = TradePair(Bitcoin, USDT)
    val ticker: Ticker = tickers(tradePair)
    val limit = ticker.priceEstimate * (1.0 + tradeRoomConfig.liquidityManager.txLimitBelowOrAboveBestBidOrAsk)
    val amountBitcoin = CryptoValue(USDT, tradeRoomConfig.pioneerOrderValueUSDT).convertTo(Bitcoin, tickers).amount

    orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeName, tradePair, TradeSide.Buy, tradeRoomConfig.exchanges(exchangeName).fee, amountBitcoin, limit)
    log.debug(s"[$exchangeName] pioneer order: ${orderRequest.shortDesc}; Ticker was: $ticker")

    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    orderRef = Await.result(
      (exchange ? NewLimitOrder(orderRequest)).mapTo[NewOrderAck],
      timeout.duration.plus(1.second))
      .toOrderRef
  }


  override def receive: Receive = {
    case Watch() => watchPioneerOrder()
  }

  def stop(): Unit = {
    watchSchedule.cancel()
    self ! PoisonPill
  }
}
