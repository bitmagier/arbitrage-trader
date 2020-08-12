package org.purevalue.arbitrage.adapter.binance

import java.time.Instant

import akka.Done
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.purevalue.arbitrage.Utils.queryJsonBinanceAccount
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.StartStreamRequest
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataChannel.BaseEndpoint
import org.purevalue.arbitrage.{AppConfig, ExchangeAccountStreamData, Main}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object BinanceAccountDataChannel {
  case class StartStreamRequest(sink: Sink[ExchangeAccountStreamData, Future[Done]])
  def props(): Props = Props(new BinanceAccountDataChannel())
}
class BinanceAccountDataChannel() extends Actor {
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val ApiKey = ???

  var initialAccountInformation: AccountInformationJson = _

  def timestamp:Long = Instant.now.toEpochMilli

  def queryAccountInformation(): AccountInformationJson = {
    implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
    import BinanceAccountDataJsonProtocoll._
    Await.result(queryJsonBinanceAccount[AccountInformationJson](s"$BaseEndpoint/api/v3/account", s"timestamp=$timestamp", ApiKey), timeout.duration)
  }

  override def preStart(): Unit = {
    initialAccountInformation = queryAccountInformation()
  }

  override def receive: Receive = {
    case StartStreamRequest(sink) =>
  }

}

case class BalanceJson(asset: String, free: String, locked: String)
case class AccountInformationJson(makerCommission: Int,
                                  takerCommission: Int,
                                  buyerCommission: Int,
                                  sellerCommission: Int,
                                  canTrade: Boolean,
                                  canWithdraw: Boolean,
                                  canDeposit: Boolean,
                                  updateTime: Long,
                                  accountType: String,
                                  balances: List[BalanceJson],
                                  permissions: List[String])
object BinanceAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val balanceJson: RootJsonFormat[BalanceJson] = jsonFormat3(BalanceJson)
  implicit val accountInformationJson: RootJsonFormat[AccountInformationJson] = jsonFormat11(AccountInformationJson)
}