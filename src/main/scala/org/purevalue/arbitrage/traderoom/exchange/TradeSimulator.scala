package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object TradeSimulator {
  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            exchange: ActorRef): Props =
    Props(new TradeSimulator(globalConfig, exchangeConfig, exchange))
}
class TradeSimulator(globalConfig: GlobalConfig,
                     exchangeConfig: ExchangeConfig,
                     exchange: ActorRef) extends Actor {
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val activeOrders: collection.concurrent.Map[String, Order] = TrieMap() // external-order-id -> Order

  def cancelOrder(tradePair: TradePair, externalOrderId: String): Future[CancelOrderResult] = {
    Future.successful {
      if (activeOrders.contains(externalOrderId)) {
        val o = activeOrders(externalOrderId)
        exchange ! IncomingAccountData(
          Seq(OrderUpdate(externalOrderId, exchangeConfig.name, tradePair, o.side, None, None, None, None, None, Some(OrderStatus.CANCELED), None, None, None, Instant.now))
        )
        activeOrders.remove(externalOrderId)
        CancelOrderResult(exchangeConfig.name, tradePair, externalOrderId, success = true, orderUnknown = false, None)
      } else {
        CancelOrderResult(exchangeConfig.name, tradePair, externalOrderId, success = false, orderUnknown = false, Some("failed because we assume the order is already filled"))
      }
    }
  }

  def newLimitOrder(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.tradePair, o.tradeSide, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.NEW), None, None, Some(o.limit), creationTime)

  def limitOrderPartiallyFilled(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.tradePair, o.tradeSide, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.PARTIALLY_FILLED), Some(o.amountBaseAsset / 2.0), None, Some(o.limit), Instant.now)

  def limitOrderFilled(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.tradePair, o.tradeSide, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.FILLED), Some(o.amountBaseAsset), None, Some(o.limit), Instant.now)

  def walletBalanceUpdate(delta: LocalCryptoValue): WalletBalanceUpdate = WalletBalanceUpdate(delta.asset, delta.amount)

  def orderLimitCloseToTickerSync(o: OrderRequest, maxDiffRate: Double): Boolean = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeout
    val tickerPrice = Await.result(
      (exchange ? GetPriceEstimate(o.tradePair)).mapTo[Double],
      timeout.duration.plus(500.millis)
    )
    val diffRate = (1.0 - tickerPrice / o.limit).abs
    diffRate < maxDiffRate
  }

  def simulateOrderLifetime(externalOrderId: String, o: OrderRequest): Unit = {
    Thread.sleep(100)
    val creationTime = Instant.now
    val limitOrder = newLimitOrder(externalOrderId, creationTime, o)
    exchange ! SimulatedAccountData(limitOrder)

    activeOrders.update(limitOrder.externalOrderId, limitOrder.toOrder)

    if (orderLimitCloseToTickerSync(o, 0.03)) {
      Thread.sleep(100)
      exchange ! SimulatedAccountData(limitOrderPartiallyFilled(externalOrderId, creationTime, o))
      val out = o.calcOutgoingLiquidity
      val outPart = LocalCryptoValue(out.exchange, out.asset, -out.amount / 2)
      val in = o.calcIncomingLiquidity
      val inPart = LocalCryptoValue(in.exchange, in.asset, in.amount / 2)
      exchange ! SimulatedAccountData(walletBalanceUpdate(outPart))
      exchange ! SimulatedAccountData(walletBalanceUpdate(inPart))

      Thread.sleep(100)
      exchange ! SimulatedAccountData(limitOrderFilled(externalOrderId, creationTime, o))
      activeOrders.remove(limitOrder.externalOrderId)
      exchange ! SimulatedAccountData(walletBalanceUpdate(outPart))
      exchange ! SimulatedAccountData(walletBalanceUpdate(inPart))
    }
  }

  def newLimitOrder(o: OrderRequest): Future[NewOrderAck] = {
    val externalOrderId = s"external-${UUID.randomUUID()}"

    executionContext.execute(() => simulateOrderLifetime(externalOrderId, o))

    Future.successful(
      NewOrderAck(exchangeConfig.name, o.tradePair, externalOrderId, o.id)
    )
  }

  override def receive: Receive = {
    case CancelOrder(ref) => cancelOrder(ref.tradePair, ref.externalOrderId).pipeTo(sender())
    case NewLimitOrder(orderRequest) => newLimitOrder(orderRequest).pipeTo(sender())
  }
}
