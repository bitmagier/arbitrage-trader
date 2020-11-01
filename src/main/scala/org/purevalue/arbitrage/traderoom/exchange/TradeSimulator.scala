package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.AccountDataChannel
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{CancelOrderResult, GetTickerSnapshot}
import org.purevalue.arbitrage.traderoom.{LocalCryptoValue, Order, OrderRequest, OrderStatus, OrderType, OrderUpdate, TradePair}
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

object TradeSimulator {
  def apply(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            exchange: ActorRef[Exchange.Message]):
  Behavior[AccountDataChannel.Command] =
    Behaviors.setup(context => new TradeSimulator(context, globalConfig, exchangeConfig, exchange))
}
class TradeSimulator(context: ActorContext[AccountDataChannel.Command],
                     globalConfig: GlobalConfig,
                     exchangeConfig: ExchangeConfig,
                     exchange: ActorRef[Exchange.Message])
  extends AccountDataChannel[AccountDataChannel.Command](context) {

  val activeOrders: collection.concurrent.Map[String, Order] = TrieMap() // external-order-id -> Order

  override def cancelOrder(pair: TradePair, externalOrderId: String): Future[Exchange.CancelOrderResult] = {
    Future.successful {
      if (activeOrders.contains(externalOrderId)) {
        val o = activeOrders(externalOrderId)
        exchange ! Exchange.IncomingAccountData(
          Seq(OrderUpdate(externalOrderId, exchangeConfig.name, pair, o.side, None, None, None, None, None, Some(OrderStatus.CANCELED), None, None, None, Instant.now))
        )
        activeOrders.remove(externalOrderId)
        CancelOrderResult(exchangeConfig.name, pair, externalOrderId, success = true, orderUnknown = false, None)
      } else {
        CancelOrderResult(exchangeConfig.name, pair, externalOrderId, success = false, orderUnknown = false, Some("failed because we assume the order is already filled"))
      }
    }
  }

  def newLimitOrder(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.pair, o.side, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.NEW), None, None, Some(o.limit), creationTime)

  def limitOrderPartiallyFilled(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.pair, o.side, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.PARTIALLY_FILLED), Some(o.amountBaseAsset / 2.0), None, Some(o.limit), Instant.now)

  def limitOrderFilled(externalOrderId: String, creationTime: Instant, o: OrderRequest): OrderUpdate =
    OrderUpdate(externalOrderId, o.exchange, o.pair, o.side, Some(OrderType.LIMIT), Some(o.limit), None, Some(o.amountBaseAsset), Some(creationTime), Some(OrderStatus.FILLED), Some(o.amountBaseAsset), None, Some(o.limit), Instant.now)

  def walletBalanceUpdate(delta: LocalCryptoValue): WalletBalanceUpdate = WalletBalanceUpdate(delta.asset, delta.amount)

  def orderLimitCloseToTickerSync(o: OrderRequest, ticker: Map[TradePair, Ticker], maxDiffRate: Double): Boolean = {
    val tickerPrice = ticker(o.pair).priceEstimate
    val diffRate = (1.0 - tickerPrice / o.limit).abs
    diffRate < maxDiffRate
  }

  def simulateOrderLifetime(externalOrderId: String, o: OrderRequest, ticker: Map[TradePair, Ticker]): Unit = {
    Thread.sleep(100)
    val creationTime = Instant.now
    val limitOrder = newLimitOrder(externalOrderId, creationTime, o)
    exchange ! Exchange.SimulatedAccountData(limitOrder)

    activeOrders.update(limitOrder.externalOrderId, limitOrder.toOrder)

    if (orderLimitCloseToTickerSync(o, ticker, 0.03)) {
      Thread.sleep(100)
      exchange ! Exchange.SimulatedAccountData(limitOrderPartiallyFilled(externalOrderId, creationTime, o))
      val out = o.calcOutgoingLiquidity
      val outPart = LocalCryptoValue(out.exchange, out.asset, -out.amount / 2)
      val in = o.calcIncomingLiquidity
      val inPart = LocalCryptoValue(in.exchange, in.asset, in.amount / 2)
      exchange ! Exchange.SimulatedAccountData(walletBalanceUpdate(outPart))
      exchange ! Exchange.SimulatedAccountData(walletBalanceUpdate(inPart))

      Thread.sleep(100)
      exchange ! Exchange.SimulatedAccountData(limitOrderFilled(externalOrderId, creationTime, o))
      activeOrders.remove(limitOrder.externalOrderId)
      exchange ! Exchange.SimulatedAccountData(walletBalanceUpdate(outPart))
      exchange ! Exchange.SimulatedAccountData(walletBalanceUpdate(inPart))
    }
  }

  def newLimitOrder(o: OrderRequest, ticker: Map[TradePair, Ticker]): Exchange.NewOrderAck = {
    val externalOrderId = s"external-${UUID.randomUUID()}"

    executionContext.execute(() => simulateOrderLifetime(externalOrderId, o, ticker))

    Exchange.NewOrderAck(exchangeConfig.name, o.pair, externalOrderId, o.id)
  }

  override def onMessage(msg: AccountDataChannel.Command): Behavior[AccountDataChannel.Command] = {
    case c: AccountDataChannel.CancelOrder =>
      handleCancelOrder(c)
      Behaviors.same

    case AccountDataChannel.NewLimitOrder(orderRequest, replyTo) =>
      implicit val timeout: Timeout = globalConfig.internalCommunicationTimeout
      exchange.ask(ref => GetTickerSnapshot(ref)).foreach { s =>
        replyTo ! newLimitOrder(orderRequest, s.ticker)
      }
      Behaviors.same
  }
}
