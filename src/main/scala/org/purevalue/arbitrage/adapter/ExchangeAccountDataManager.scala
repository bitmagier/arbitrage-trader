package org.purevalue.arbitrage.adapter

import java.util.UUID

import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager._
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{ExchangeAccountDataChannelInit, OrderUpdateTrigger, WalletUpdateTrigger}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, TradeRoomConfig}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.duration.DurationInt


object ExchangeAccountDataManager {
  case class IncomingData(data: Seq[ExchangeAccountStreamData])
  case class Initialized()
  case class CancelOrder(ref: OrderRef)
  case class CancelOrderResult(exchange: String, tradePair: TradePair, externalOrderId: String, success: Boolean, text: Option[String])
  case class NewLimitOrder(orderRequest: OrderRequest) // response is NewOrderAck
  case class NewOrderAck(exchange: String, tradePair: TradePair, externalOrderId: String, orderId: UUID) {
    def toOrderRef: OrderRef = OrderRef(exchange, tradePair, externalOrderId)
  }
  case class SimulatedData(dataset: ExchangeAccountStreamData)

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            tradeRoomConfig: TradeRoomConfig,
            exchange: ActorRef,
            exchangePublicDataInquirer: ActorRef,
            exchangeAccountDataChannelInit: ExchangeAccountDataChannelInit,
            accountData: ExchangeAccountData): Props =
    Props(new ExchangeAccountDataManager(globalConfig, exchangeConfig, tradeRoomConfig, exchange, exchangePublicDataInquirer, exchangeAccountDataChannelInit, accountData))
}
class ExchangeAccountDataManager(globalConfig: GlobalConfig,
                                 exchangeConfig: ExchangeConfig,
                                 tradeRoomConfig: TradeRoomConfig,
                                 exchange: ActorRef,
                                 exchangePublicDataInquirer: ActorRef,
                                 exchangeAccountDataChannelInit: ExchangeAccountDataChannelInit,
                                 accountData: ExchangeAccountData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeAccountDataManager])

  var accountDataChannel: ActorRef = _

  private def applyData(data: ExchangeAccountStreamData): Unit = {
    if (log.isTraceEnabled) log.trace(s"applying incoming $data")

    data match {
      case w: WalletBalanceUpdate =>
        accountData.wallet.balance = accountData.wallet.balance.map {
          case (a: Asset, b: Balance) if a == w.asset =>
            (a, Balance(b.asset, b.amountAvailable + w.amountDelta, b.amountLocked))
          case (a: Asset, b: Balance) => (a, b)
        }
        exchange ! WalletUpdateTrigger()

      case w: WalletAssetUpdate =>
        accountData.wallet.balance = accountData.wallet.balance ++ w.balance
        exchange ! WalletUpdateTrigger()

      case w: CompleteWalletUpdate =>
        if (w.balance != accountData.wallet) { // we get snapshots delivered here, so updates are needed only, when something changed
          accountData.wallet.balance = w.balance
          exchange ! WalletUpdateTrigger()
        }

      case o: OrderUpdate =>
        val ref = OrderRef(exchangeConfig.name, o.tradePair, o.externalOrderId)
        if (accountData.activeOrders.contains(ref)) {
          accountData.activeOrders(ref).applyUpdate(o)
        } else {
          accountData.activeOrders.update(ref, o.toOrder) // covers a restart-scenario (tradeRoom / arbitrage-trader)
        }
        exchange ! OrderUpdateTrigger(ref)

      case _ => throw new NotImplementedError
    }
  }

  def applySimulatedData(dataset: ExchangeAccountStreamData): Unit = {
    if (!tradeRoomConfig.tradeSimulation) throw new RuntimeException
    log.trace(s"Applying simulation data ...")
    applyData(dataset)
  }

  def cancelOrderIfStillExist(c: CancelOrder): Unit = {
    if (accountData.activeOrders.contains(c.ref)) {
      accountDataChannel.forward(c)
    }
  }

  override val supervisorStrategy: OneForOneStrategy = {
    OneForOneStrategy(maxNrOfRetries = 6, withinTimeRange = 30.minutes, loggingEnabled = true) {
      case _: Throwable => Restart
    }
  }

  override def preStart(): Unit = {
    accountDataChannel = context.actorOf(exchangeAccountDataChannelInit(globalConfig, exchangeConfig, self, exchangePublicDataInquirer),
      s"${exchangeConfig.name}.AccountDataChannel")
  }

  override def receive: Receive = {
    case i: Initialized =>
      exchange.forward(i) // is expected to come from exchange specific account-data-channel when initialized
      context.become(initializedModeReceive)
    case IncomingData(data) => data.foreach(applyData)
    case Failure(e) => log.error("received failure", e)
  }


  // @formatter:off
  def initializedModeReceive: Receive = {
    case IncomingData(data)     => data.foreach(applyData)
    case c: CancelOrder         => cancelOrderIfStillExist(c)
    case o: NewLimitOrder       => accountDataChannel.forward(o)
    case SimulatedData(dataset) => applySimulatedData(dataset)
    case Failure(e)             => log.error("received failure", e)
  }
  // @formatter:on
}


trait ExchangeAccountStreamData

case class Balance(asset: Asset, amountAvailable: Double, amountLocked: Double) {
  override def toString: String = s"Balance(${asset.officialSymbol}: " +
    s"available:${formatDecimal(amountAvailable, asset.defaultFractionDigits)}, " +
    s"locked: ${formatDecimal(amountLocked, asset.defaultFractionDigits)})"
}
// we use a [var immutable map] instead of mutable one here, to be able to update the whole map at once without a race condition
case class Wallet(exchange: String, @volatile var balance: Map[Asset, Balance], exchangeConfig: ExchangeConfig) {

  def toOverviewString(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): String = {
    val liquidity = this.liquidCryptoValueSum(aggregateAsset, ticker)
    val inconvertible = this.inconvertibleCryptoValues(aggregateAsset, ticker)
    s"Wallet [$exchange]: Liquid crypto total: $liquidity" +
      (if (inconvertible.nonEmpty) s""", Inconvertible to ${aggregateAsset.officialSymbol}: ${inconvertible.mkString(", ")}""" else "") +
      (if (this.fiatMoney.nonEmpty) s""", Fiat Money: ${this.fiatMoney.mkString(", ")}""" else "") +
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

  // "liquid crypto values" are our wallet value of crypt assets, which are available for trading and converting-calculations
  def liquidCryptoValues(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): Iterable[CryptoValue] =
    balance
      .filterNot(_._1.isFiat)
      .filterNot(b => exchangeConfig.doNotTouchTheseAssets.contains(b._1))
      .map(b => CryptoValue(b._1, b._2.amountAvailable))
      .filter(_.canConvertTo(aggregateAsset, ticker))

  def liquidCryptoValueSum(aggregateAsset: Asset, ticker: collection.Map[TradePair, Ticker]): CryptoValue = {
    liquidCryptoValues(aggregateAsset, ticker)
      .map(_.convertTo(aggregateAsset, ticker))
      .foldLeft(CryptoValue(aggregateAsset, 0.0))((a, x) => CryptoValue(a.asset, a.amount + x.amount))
  }
}

case class CompleteWalletUpdate(balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
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

case class ExchangeAccountData(wallet: Wallet,
                               activeOrders: concurrent.Map[OrderRef, Order])
