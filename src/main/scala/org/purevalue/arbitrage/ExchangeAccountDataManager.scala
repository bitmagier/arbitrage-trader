package org.purevalue.arbitrage

import java.util.UUID

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.stream.scaladsl.Sink
import org.purevalue.arbitrage.ExchangeAccountDataManager.{CancelOrder, FetchOrder, NewLimitOrder, SimulatedData}
import org.purevalue.arbitrage.TradeRoom.{OrderRef, OrderUpdateTrigger, WalletUpdateTrigger}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.StartStreamRequest
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.Future


object ExchangeAccountDataManager {
  case class FetchOrder(tradePair: TradePair, externalOrderId: String) // order shall be queried from exchange and feed into the data stream (e.g. for refreshing persisted open orders after restart)
  case class CancelOrder(tradePair: TradePair, externalOrderId: String)
  case class CancelOrderResult(tradePair: TradePair, externalOrderId: String, success: Boolean)
  case class NewLimitOrder(o: OrderRequest) // response is NewOrderAck
  case class NewOrderAck(exchange: String, tradePair: TradePair, externalOrderId: String, orderId: UUID) {
    def toOrderRef: OrderRef = OrderRef(exchange, tradePair, externalOrderId)
  }
  case class SimulatedData(dataset: ExchangeAccountStreamData)

  def props(config: ExchangeConfig,
            exchangePublicDataInquirer: ActorRef,
            tradeRoom: ActorRef,
            exchangeAccountDataChannelInit: Function2[ExchangeConfig, ActorRef, Props],
            accountData: IncomingExchangeAccountData): Props =
    Props(new ExchangeAccountDataManager(config, exchangePublicDataInquirer, tradeRoom, exchangeAccountDataChannelInit, accountData))
}
class ExchangeAccountDataManager(config: ExchangeConfig,
                                 exchangePublicDataInquirer: ActorRef,
                                 tradeRoom: ActorRef,
                                 exchangeAccountDataChannelInit: Function2[ExchangeConfig, ActorRef, Props],
                                 accountData: IncomingExchangeAccountData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeAccountDataManager])
  var accountDataChannel: ActorRef = _

  val sink: Sink[ExchangeAccountStreamData, Future[Done]] = Sink.foreach[ExchangeAccountStreamData] { x =>
    if (log.isTraceEnabled()) log.trace(s"${config.exchangeName}: received $x")
    applyData(x)
  }

  private def applyData(dataset: ExchangeAccountStreamData): Unit = {
    dataset match {
      case w: Wallet =>
        accountData.wallet.balance = w.balance
        tradeRoom ! WalletUpdateTrigger(config.exchangeName)

      case w: WalletAssetUpdate =>
        accountData.wallet.balance = accountData.wallet.balance -- w.balance.keys ++ w.balance
        tradeRoom ! WalletUpdateTrigger(config.exchangeName)

      case w: WalletBalanceUpdate =>
        accountData.wallet.balance = accountData.wallet.balance.map {
          // TODO validate if an update of amountAvailable is the right thing, that is meant by this message (I'm 95% sure so far)
          case (a: Asset, b: Balance) if a == w.asset =>
            (a, Balance(b.asset, b.amountAvailable + w.amountDelta, b.amountLocked))
          case x => x
        }
        tradeRoom ! WalletUpdateTrigger(config.exchangeName)

      case o: Order =>
        val ref = o.ref
        accountData.activeOrders.update(ref, o)
        tradeRoom ! OrderUpdateTrigger(ref)

      case o: OrderUpdate =>
        val ref = OrderRef(config.exchangeName, o.tradePair, o.externalOrderId)
        if (accountData.activeOrders.contains(ref))
          accountData.activeOrders(ref).applyUpdate(o)
        else
          accountData.activeOrders.update(ref, o.toOrder(config.exchangeName)) // covers a restart-scenario (tradeRoom / arbitrage-trader)

        tradeRoom ! OrderUpdateTrigger(ref)

      case _ => throw new NotImplementedError
    }
  }

  override def preStart(): Unit = {
    accountDataChannel = context.actorOf(exchangeAccountDataChannelInit(config, exchangePublicDataInquirer), s"${config.exchangeName}.AccountDataChannel")
    accountDataChannel ! StartStreamRequest(sink)
  }

  override def receive: Receive = {
    case f: FetchOrder => accountDataChannel.forward(f)
    case c: CancelOrder => accountDataChannel.forward(c)
    case o: NewLimitOrder => accountDataChannel.forward(o)

    case SimulatedData(dataset) =>
      if (!Config.tradeRoom.tradeSimulation) throw new RuntimeException
      log.debug(s"Applying simulation data: $dataset")
      applyData(dataset)
  }
}


trait ExchangeAccountStreamData

case class Balance(asset: Asset, amountAvailable: Double, amountLocked: Double) {
  def toCryptoValue: CryptoValue = CryptoValue(asset, amountAvailable)

  override def toString: String = s"Balance(${asset.officialSymbol}: " +
    s"available:${formatDecimal(amountAvailable, asset.visibleAmountFractionDigits)}, " +
    s"locked: ${formatDecimal(amountLocked, asset.visibleAmountFractionDigits)})"
}
// we use a [var immutable map] instead of mutable one here, to be able to update the whole map at once without a race condition
case class Wallet(var balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
case class WalletAssetUpdate(balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
case class WalletBalanceUpdate(asset: Asset, amountDelta: Double) extends ExchangeAccountStreamData

/**
 * @see https://academy.binance.com/economics/what-are-makers-and-takers
 */
case class Fee(exchange: String,
               makerFee: Double,
               takerFee: Double) {
  def average: Double = (makerFee + takerFee) / 2
}

case class IncomingExchangeAccountData(wallet: Wallet, activeOrders: concurrent.Map[OrderRef, Order])
