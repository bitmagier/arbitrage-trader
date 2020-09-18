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
import org.purevalue.arbitrage.{GlobalConfig, Main, TradeRoomConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}


// run a validated pioneer transaction before the exchange gets available for further orders
// we buy BTC from USDT (configured amount)
// TODO add another order, to check if cancel-ordewr works
object PioneerOrderRunner {
  case class Watch()
  case class PioneerOrderSucceeded()
  case class PioneerOrderFailed(exception:Exception)

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
    def failed = throw new RuntimeException(s"Pioneer order validation failed! \n$request, \n$finishedOrder")

    if (finishedOrder.exchange != request.exchange) failed
    if (finishedOrder.side != request.tradeSide) failed
    if (finishedOrder.tradePair != request.tradePair) failed
    if (request.tradeSide == TradeSide.Buy && (finishedOrder.priceAverage > request.limit)) failed
    if (request.tradeSide == TradeSide.Sell && (finishedOrder.priceAverage < request.limit)) failed
    if (finishedOrder.orderPrice != request.limit) failed
    if (finishedOrder.orderType != OrderType.LIMIT) failed
    if (finishedOrder.orderStatus != OrderStatus.FILLED) failed
    if (finishedOrder.quantity != request.amountBaseAsset) failed
    if (finishedOrder.cumulativeFilledQuantity != request.amountBaseAsset) failed
    val sumUSDT = OrderBill.aggregateValues(
      OrderBill.calcBalanceSheet(finishedOrder, Fee(exchangeName, 0.0, 0.0)),
      USDT,
      (_,tradePair) => tickers.get(tradePair).map(_.priceEstimate))
    if (sumUSDT < -0.01) failed // more than 1 cent loss is absolutely unacceptable
  }

  def watchPioneerTransaction(): Unit = {
    if (Instant.now.isAfter(deadline)) {
      exchange ! PioneerOrderFailed(new RuntimeException("Timeout while waiting for pioneer transaction to complete"))
      self ! PoisonPill
    }

    activeOrderLookup(orderRef) match {
      case Some(order) if !order.orderStatus.isFinal =>
        log.debug(s"pioneer order in progress")

      case Some(order) =>
        try {
          validateFinishedPioneerOrder(orderRequest, order)
          log.info(s"pioneer transaction for $exchangeName succeeded")
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
    val limit = tickers(tradePair).priceEstimate
    val amountBitcoin = CryptoValue(USDT, tradeRoomConfig.pioneerOrderValueUSDT).convertTo(Bitcoin, tickers).amount
    orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeName, tradePair, TradeSide.Buy, tradeRoomConfig.exchanges(exchangeName).fee, amountBitcoin, limit)

    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    orderRef = Await.result(
      (exchange ? NewLimitOrder(orderRequest)).mapTo[NewOrderAck],
      timeout.duration.plus(1.second))
      .toOrderRef
  }


  override def receive: Receive = {
    case Watch() => watchPioneerTransaction()
  }

  def stop(): Unit = {
    watchSchedule.cancel()
    self ! PoisonPill
  }
}
