package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager._
import org.purevalue.arbitrage.adapter.WalletBalanceUpdate
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.{ExchangeConfig, adapter}

import scala.concurrent.{ExecutionContextExecutor, Future}

object TradeSimulator {
  def props(exchangeConfig: ExchangeConfig,
            accountDataManager: ActorRef): Props =
    Props(new TradeSimulator(exchangeConfig, accountDataManager))
}
class TradeSimulator(exchangeConfig: ExchangeConfig,
                     accountDataManager: ActorRef) extends Actor {
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  def cancelOrder(tradePair: TradePair, externalOrderId: String): Future[CancelOrderResult] = {
    Future.successful(
      CancelOrderResult(exchangeConfig.name, tradePair, externalOrderId, success = false, Some("(easy way) always fail, because we assume the order is already filled"))
    )
  }

  def newLimitOrder(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.tradePair, o.tradeSide, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.NEW), None, None, Some(o.limit), creationTime)

  def limitOrderPartiallyFilled(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.tradePair, o.tradeSide, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.PARTIALLY_FILLED), Some(o.amountBaseAsset / 2.0), None, Some(o.limit), Instant.now)

  def limitOrderFilled(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.tradePair, o.tradeSide, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.FILLED), Some(o.amountBaseAsset), None, Some(o.limit), Instant.now)

  def walletBalanceUpdate(delta: LocalCryptoValue): WalletBalanceUpdate = adapter.WalletBalanceUpdate(delta.asset, delta.amount)

  def simulateOrderLifetime(externalOrderId: String, o: OrderRequest): Unit = {
    Thread.sleep(100)
    val creationTime = Instant.now
    accountDataManager ! SimulatedData(newLimitOrder(externalOrderId, creationTime, o))
    Thread.sleep(100)
    accountDataManager ! SimulatedData(limitOrderPartiallyFilled(externalOrderId, creationTime, o))
    val out = o.calcOutgoingLiquidity
    val outPart = LocalCryptoValue(out.exchange, out.asset, -out.amount / 2)
    val in = o.calcIncomingLiquidity
    val inPart = LocalCryptoValue(in.exchange, in.asset, in.amount / 2)
    accountDataManager ! SimulatedData(walletBalanceUpdate(outPart))
    accountDataManager ! SimulatedData(walletBalanceUpdate(inPart))

    Thread.sleep(100)
    accountDataManager ! SimulatedData(limitOrderFilled(externalOrderId, creationTime, o))
    accountDataManager ! SimulatedData(walletBalanceUpdate(outPart))
    accountDataManager ! SimulatedData(walletBalanceUpdate(inPart))
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
