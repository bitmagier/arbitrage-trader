//package org.purevalue.arbitrage.adapter.binance
//
//import java.time.Instant
//
//import akka.actor.{Actor, ActorSystem, Cancellable, Props}
//import akka.stream.{Graph, OverflowStrategy}
//import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
//import akka.{Done, NotUsed}
//import org.purevalue.arbitrage.HttpUtils.queryJsonBinanceAccount
//import org.purevalue.arbitrage._
//import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.{QueryData, StartStreamRequest}
//import org.purevalue.arbitrage.adapter.binance.BinancePublicDataChannel.BaseEndpoint
//import spray.json.{DefaultJsonProtocol, RootJsonFormat}
//
//import scala.concurrent.duration.DurationInt
//import scala.concurrent.{Await, ExecutionContextExecutor, Future}
//
//object BinanceAccountDataChannel {
//  case class StartStreamRequest(sink: Sink[ExchangeAccountStreamData, Future[Done]])
//  case class QueryData()
//
//  def props(config: ExchangeConfig): Props = Props(new BinanceAccountDataChannel(config))
//}
//class BinanceAccountDataChannel(config: ExchangeConfig) extends Actor {
//  implicit val system: ActorSystem = Main.actorSystem
//  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//
//  val querySchedule: Cancellable = system.scheduler.scheduleWithFixedDelay(0.seconds, 1.second, self, QueryData())
//
//  var initialAccountInformation: AccountInformationJson = _
//
//  val sourceQueue: Source[IncomingBinanceAccountJson, SourceQueueWithComplete[IncomingBinanceAccountJson]] =
//    Source.queue[IncomingBinanceAccountJson](2, OverflowStrategy.backpressure)
//
//  val downStreamFlow: Flow[IncomingBinanceAccountJson, ExchangeAccountStreamData, NotUsed] = Flow.fromFunction {
//    case j:AccountInformationJson => j.toWallet
//    case _ => throw new NotImplementedError
//  }
//
//  def timestamp:Long = Instant.now.toEpochMilli
//
//  def queryAccountInformation(): AccountInformationJson = {
//    import BinanceAccountDataJsonProtocoll._
//    Await.result(
//      queryJsonBinanceAccount[AccountInformationJson](s"$BaseEndpoint/api/v3/account", s"timestamp=$timestamp", config.secrets),
//      Config.httpTimeout)
//  }
//
//  override def preStart(): Unit = {
//    sourceQueue
////    initialAccountInformation = queryAccountInformation()
//  }
//
//  override def receive: Receive = {
//    case StartStreamRequest(sink) => // TODO connect sink to 2 sources: REST + WebSocket
//      Flow.fromSinkAndSource(
//        downStreamFlow.toMat(sink)(Keep.right),
//        sourceQueue)
//
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
//
//// TODO replace repeated REST calls by corresponding WebSocket Streams