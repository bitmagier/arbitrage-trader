package org.purevalue.arbitrage

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.stream.scaladsl.Sink
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.StartStreamRequest
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.concurrent.Future

trait ExchangeAccountStreamData

case class Balance(asset: Asset, amountAvailable: Double, amountLocked: Double) {
  def toCryptoValue: CryptoValue = CryptoValue(asset, amountAvailable)
}
// we use a [var immutable map] instead of mutable one here, to be able to update the whole map at once without a race condition
case class Wallet(var balances: Map[Asset, Balance]) extends ExchangeAccountStreamData
case class WalletAssetUpdate(balances: Map[Asset, Balance]) extends ExchangeAccountStreamData
case class WalletBalanceUpdate(asset:Asset, balanceDelta:Double) extends ExchangeAccountStreamData


case class Fee(exchange: String,
               makerFee: Double,
               takerFee: Double)


case class ExchangeAccountData(wallet: Wallet)

object ExchangeAccountDataManager {
  def props(config: ExchangeConfig,
            exchangeAccountDataChannelInit: () => Props,
            accountData: ExchangeAccountData): Props =
    Props(new ExchangeAccountDataManager(config, exchangeAccountDataChannelInit, accountData))
}
class ExchangeAccountDataManager(config: ExchangeConfig,
                                 exchangeAccountDataChannelInit: () => Props,
                                 accountData: ExchangeAccountData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeAccountDataManager])
  var accountDataChannel: ActorRef = _

  val sink: Sink[ExchangeAccountStreamData, Future[Done]] = Sink.foreach[ExchangeAccountStreamData] {
    case w: Wallet =>
      if (log.isTraceEnabled()) log.trace(s"${config.exchangeName}: received $w")
      accountData.wallet.balances = w.balances
    case w: WalletAssetUpdate =>
      if (log.isTraceEnabled()) log.trace(s"${config.exchangeName}: received $w")
      accountData.wallet.balances = accountData.wallet.balances -- w.balances.keys ++ w.balances
    case w: WalletBalanceUpdate =>
      if (log.isTraceEnabled()) log.trace(s"${config.exchangeName}: received $w")
      accountData.wallet.balances = accountData.wallet.balances.map {
        // TODO validate if an update of amountAvailable is the right thing, that is meant by this message (I'm 95% sure so far)
        case (a:Asset,b:Balance) if a == w.asset =>
          (a, Balance(b.asset, b.amountAvailable + w.balanceDelta, b.amountLocked))
        case x => x
      }
    // ...
    case _ => throw new NotImplementedError
  }

  override def preStart(): Unit = {
    accountDataChannel = context.actorOf(exchangeAccountDataChannelInit.apply(), s"${config.exchangeName}.AccountDataChannel")
    accountDataChannel ! StartStreamRequest(sink)
  }

  override def receive: Receive = {
    case _ =>
  }
}
