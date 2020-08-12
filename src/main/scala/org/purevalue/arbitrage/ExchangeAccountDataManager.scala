package org.purevalue.arbitrage

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.stream.scaladsl.Sink
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.StartStreamRequest

import scala.collection.{Map, Seq}
import scala.concurrent.Future

trait ExchangeAccountStreamData

case class Balance(asset: Asset, amountAvailable: Double, amountLocked: Double) {
  def toCryptoValue: CryptoValue = CryptoValue(asset, amountAvailable)
}
// we use a [var immutable map] instead of mutable one here, to be able to update the whole map at once without race a condition
case class Wallet(var balances: Map[Asset, Balance]) extends ExchangeAccountStreamData


case class Fee(exchange: String,
               makerFee: Double,
               takerFee: Double)


case class ExchangeAccountData(wallet: Wallet)

object ExchangeAccountDataManager {
  def props(config: ExchangeConfig, accountData: ExchangeAccountData): Props = Props(new ExchangeAccountDataManager(config, accountData))
}
class ExchangeAccountDataManager(config: ExchangeConfig,
                                 accountData: ExchangeAccountData) extends Actor {
  var accountDataChannel: ActorRef = _

  val sink: Sink[ExchangeAccountStreamData, Future[Done]] = Sink.foreach[ExchangeAccountStreamData] {

    case w: Wallet =>
      accountData.wallet.balances = w.balances

    case _ => throw new NotImplementedError
  }

  override def preStart(): Unit = {
    accountDataChannel = context.actorOf(???, s"${config.exchangeName}.AccountDataChannel")
    accountDataChannel ! StartStreamRequest(sink)
  }

  override def receive: Receive = ???

}
