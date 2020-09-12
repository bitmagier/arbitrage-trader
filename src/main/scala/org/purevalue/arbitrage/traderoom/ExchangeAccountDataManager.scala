package org.purevalue.arbitrage.traderoom

import java.util.UUID

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.stream.scaladsl.Sink
import org.purevalue.arbitrage.traderoom.ExchangeAccountDataManager._
import org.purevalue.arbitrage.traderoom.TradeRoom.{OrderRef, OrderUpdateTrigger, TickersReadonly, WalletUpdateTrigger}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{Config, ExchangeConfig}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.Future


object ExchangeAccountDataManager {
  case class StartStreamRequest(sink: Sink[Seq[ExchangeAccountStreamData], Future[Done]])
  case class Initialized()
  case class CancelOrder(tradePair: TradePair, externalOrderId: String)
  case class CancelOrderResult(tradePair: TradePair, externalOrderId: String, success: Boolean)
  case class NewLimitOrder(o: OrderRequest) // response is NewOrderAck
  case class NewOrderAck(exchange: String, tradePair: TradePair, externalOrderId: String, orderId: UUID) {
    def toOrderRef: OrderRef = OrderRef(exchange, tradePair, externalOrderId)
  }
  case class SimulatedData(dataset: ExchangeAccountStreamData)

  def props(config: ExchangeConfig,
            exchange: ActorRef,
            exchangePublicDataInquirer: ActorRef,
            tradeRoom: ActorRef,
            exchangeAccountDataChannelInit: Function2[ExchangeConfig, ActorRef, Props],
            accountData: ExchangeAccountData): Props =
    Props(new ExchangeAccountDataManager(config, exchange, exchangePublicDataInquirer, tradeRoom, exchangeAccountDataChannelInit, accountData))
}
class ExchangeAccountDataManager(config: ExchangeConfig,
                                 exchange: ActorRef,
                                 exchangePublicDataInquirer: ActorRef,
                                 tradeRoom: ActorRef,
                                 exchangeAccountDataChannelInit: Function2[ExchangeConfig, ActorRef, Props],
                                 accountData: ExchangeAccountData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeAccountDataManager])
  var accountDataChannel: ActorRef = _

  val sink: Sink[Seq[ExchangeAccountStreamData], Future[Done]] = Sink.foreach[Seq[ExchangeAccountStreamData]] { data =>
    data.foreach(applyData)
  }

  private def applyData(dataset: ExchangeAccountStreamData): Unit = {

    if (log.isTraceEnabled) log.trace(s"applying incoming $dataset")

    dataset match {

      case w: WalletAssetUpdate =>
        accountData.wallet.balance = accountData.wallet.balance -- w.balance.keys ++ w.balance
        tradeRoom ! WalletUpdateTrigger(config.exchangeName)

      case w: WalletBalanceUpdate =>
        accountData.wallet.balance = accountData.wallet.balance.map {
          // TODO validate if an update of amountAvailable is the right thing, that is meant by this message (I'm 95% sure so far)
          case (a: Asset, b: Balance) if a == w.asset =>
            (a, Balance(b.asset, b.amountAvailable + w.amountDelta, b.amountLocked))
          case (a: Asset, b: Balance) => (a, b)
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
    case i: Initialized => exchange.forward(i)
    case c: CancelOrder => accountDataChannel.forward(c)
    case o: NewLimitOrder => accountDataChannel.forward(o)

    case SimulatedData(dataset) =>
      if (!Config.tradeRoom.tradeSimulation) throw new RuntimeException
      log.trace(s"Applying simulation data ...")
      applyData(dataset)
  }
}


trait ExchangeAccountStreamData

case class Balance(asset: Asset, amountAvailable: Double, amountLocked: Double) {
  override def toString: String = s"Balance(${asset.officialSymbol}: " +
    s"available:${formatDecimal(amountAvailable, asset.visibleAmountFractionDigits)}, " +
    s"locked: ${formatDecimal(amountLocked, asset.visibleAmountFractionDigits)})"
}
// we use a [var immutable map] instead of mutable one here, to be able to update the whole map at once without a race condition
case class Wallet(exchange: String, var balance: Map[Asset, Balance], exchangeConfig: ExchangeConfig) {
  def toOverviewString(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): String = {
    val liquidity = this.liquidCryptoValueSum(aggregateAsset, ticker)
    val inconvertible = this.inconvertibleCryptoValues(aggregateAsset, ticker)
    s"Wallet [$exchange]: Liquid crypto total: $liquidity" +
      (if (inconvertible.nonEmpty) s""", Inconvertible to ${aggregateAsset.officialSymbol}: ${inconvertible.mkString(", ")}""" else "") +
      s""", Fiat Money: ${this.fiatMoney.mkString(", ")}""" +
      (if (this.notTouchValues.nonEmpty) s""", Not-touching: ${this.notTouchValues.mkString(", ")}""" else "")
  }

  override def toString: String = s"""Wallet($exchange, ${balance.mkString(",")})"""

  def inconvertibleCryptoValues(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): Seq[CryptoValue] =
    balance
      .filterNot(_._1.isFiat)
      .map(e => CryptoValue(e._1, e._2.amountAvailable))
      .filter(e => !e.canConvertTo(aggregateAsset, ticker))
      .toSeq
      .sortBy(_.asset.officialSymbol)

  def notTouchValues: Seq[CryptoValue] =
    balance
      .filter(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
      .map(e => CryptoValue(e._1, e._2.amountAvailable))
      .toSeq
      .sortBy(_.asset.officialSymbol)

  def fiatMoney: Seq[FiatMoney] =
    balance
      .filter(_._1.isFiat)
      .map(e => FiatMoney(e._1, e._2.amountAvailable))
      .toSeq
      .sortBy(_.asset.officialSymbol)

  private def liquidCryptoValues(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): Iterable[CryptoValue] =
    balance
      .filter(b => !b._1.isFiat)
      .filterNot(b => exchangeConfig.doNotTouchTheseAssets.contains(b._1))
      .map(b => CryptoValue(b._1, b._2.amountAvailable))
      .filter(_.canConvertTo(aggregateAsset, ticker))

  def liquidCryptoValueSum(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): CryptoValue = {
    liquidCryptoValues(aggregateAsset, ticker)
      .map(_.convertTo(aggregateAsset, ticker))
      .foldLeft(CryptoValue(aggregateAsset, 0.0))((a, x) => CryptoValue(a.asset, a.amount + x.amount))
  }
}

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

case class ExchangeAccountData(wallet: Wallet, activeOrders: concurrent.Map[OrderRef, Order])
