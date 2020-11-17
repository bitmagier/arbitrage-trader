package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.AccountDataChannel
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{CancelOrderResult, GetTickerSnapshot}
import org.purevalue.arbitrage.traderoom.{LocalCryptoValue, Order, OrderRequest, OrderStatus, OrderType, OrderUpdate, TradePair}
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.{Failure, Success}

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
  extends AccountDataChannel(context) {

  private val log = LoggerFactory.getLogger(getClass)

  import AccountDataChannel._

  val activeOrders: collection.concurrent.Map[String, Order] = TrieMap() // external-order-id -> Order

  override def cancelOrder(ref: OrderRef): Future[Exchange.CancelOrderResult] = {
    Future.successful {
      if (activeOrders.contains(ref.externalOrderId)) {
        val o = activeOrders(ref.externalOrderId)
        exchange ! Exchange.IncomingAccountData(
          Seq(OrderUpdate(ref.externalOrderId, exchangeConfig.name, ref.pair, o.side, None, None, None, None, None, Some(OrderStatus.CANCELED), None, None, None, Instant.now))
        )
        activeOrders.remove(ref.externalOrderId)
        CancelOrderResult(exchangeConfig.name, ref.pair, ref.externalOrderId, success = true, orderUnknown = false, None)
      } else {
        CancelOrderResult(exchangeConfig.name, ref.pair, ref.externalOrderId, success = false, orderUnknown = false, Some("failed because we assume the order is already filled"))
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

  override def onMessage(message: Command): Behavior[Command] = message match {
    case c: AccountDataChannel.CancelOrder =>
      handleCancelOrder(c)
      Behaviors.same

    case AccountDataChannel.NewLimitOrder(orderRequest, replyTo) =>
      implicit val timeout: Timeout = globalConfig.internalCommunicationTimeout
      exchange.ask(ref => GetTickerSnapshot(ref)).onComplete {
        case Success(s) => replyTo ! newLimitOrder(orderRequest, s.ticker)
        case Failure(e) => log.error("GetTickerSnapshot failed", e)
      }
      Behaviors.same
  }
}
