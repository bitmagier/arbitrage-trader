//package org.purevalue.arbitrage.adapter.binance
//
//import java.time.Instant
//
//import akka.actor.{Actor, ActorSystem, Props}
//import akka.stream.OverflowStrategy
//import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
//import akka.{Done, NotUsed}
//import org.purevalue.arbitrage.HttpUtils.queryJsonBinanceAccount
//import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.StartStreamRequest
//import org.purevalue.arbitrage.adapter.binance.BinancePublicDataChannel.BaseEndpoint
//import org.purevalue.arbitrage._
//import spray.json.{DefaultJsonProtocol, RootJsonFormat}
//
//import scala.concurrent.{Await, ExecutionContextExecutor, Future}
//
//object BinanceAccountDataChannel {
//  case class StartStreamRequest(sink: Sink[ExchangeAccountStreamData, Future[Done]])
//
//  def props(config: ExchangeConfig): Props = Props(new BinanceAccountDataChannel(config))
//}
//class BinanceAccountDataChannel(config: ExchangeConfig) extends Actor {
//  implicit val system: ActorSystem = Main.actorSystem
//  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//
//  var initialAccountInformation: AccountInformationJson = _
//
//
//
//  val sourceQueue: Source[IncomingBinanceAccountJson, SourceQueueWithComplete[IncomingBinanceAccountJson]] =
//    Source.queue[IncomingBinanceAccountJson](10, OverflowStrategy.backpressure)
//
//  val webSocketSource: Source[IncomingBinanceAccountJson, NotUsed] = ???
//
//  val downStreamFlow: Flow[IncomingBinanceAccountJson, Option[ExchangeAccountStreamData], NotUsed] = Flow.fromFunction {
//
//    case j:AccountInformationJson => Some(j.toWallet)
//
//    case _ => throw new NotImplementedError
//  }
//
//  def timestamp:Long = Instant.now.toEpochMilli
//
//  def queryAccountInformation(): AccountInformationJson = {
//    import BinanceAccountDataJsonProtocoll._
//    Await.result(
//      queryJsonBinanceAccount[AccountInformationJson](s"$BaseEndpoint/api/v3/account", s"timestamp=$timestamp", config.tradingSecrets),
//      AppConfig.httpTimeout)
//  }
//
//  override def preStart(): Unit = {
//    sourceQueue
////    initialAccountInformation = queryAccountInformation()
//  }
//
//  override def receive: Receive = {
//    case StartStreamRequest(sink) =>
//  }
//
//}
//
//trait IncomingBinanceAccountJson
//case class BalanceJson(asset: String, free: String, locked: String) {
//  def toBalance: Balance = Balance(
//    Asset(asset),
//    free.toDouble,
//    locked.toDouble
//  )
//}
//case class AccountInformationJson(makerCommission: Int,
//                                  takerCommission: Int,
//                                  buyerCommission: Int,
//                                  sellerCommission: Int,
//                                  canTrade: Boolean,
//                                  canWithdraw: Boolean,
//                                  canDeposit: Boolean,
//                                  updateTime: Long,
//                                  accountType: String,
//                                  balances: List[BalanceJson],
//                                  permissions: List[String]) extends IncomingBinanceAccountJson {
//  def toWallet: ExchangeAccountStreamData =
//    Wallet(balances
//      .map(_.toBalance)
//      .map(e => (e.asset, e))
//      .toMap)
//}
//object BinanceAccountDataJsonProtocoll extends DefaultJsonProtocol {
//  implicit val balanceJson: RootJsonFormat[BalanceJson] = jsonFormat3(BalanceJson)
//  implicit val accountInformationJson: RootJsonFormat[AccountInformationJson] = jsonFormat11(AccountInformationJson)
//}