package org.purevalue.arbitrage.adapter.binance

import java.time.Instant

import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.stream.{Graph, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.HttpUtils.httpRequestJsonBinanceAccount
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.{QueryData, SendPing, StartStreamRequest}
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataChannel.BaseRestEndpoint
import org.purevalue.arbitrage.adapter.binance.WebSocketJsonProtocoll.subscribeMsg
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

object BinanceAccountDataChannel {
  case class StartStreamRequest(sink: Sink[ExchangeAccountStreamData, NotUsed])
  case class QueryData()
  case class SendPing()

  def props(config: ExchangeConfig): Props = Props(new BinanceAccountDataChannel(config))
}
class BinanceAccountDataChannel(config: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceAccountDataChannel])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val querySchedule: Cancellable = system.scheduler.scheduleWithFixedDelay(0.seconds, 1.second, self, QueryData())
  val pingSchedule: Cancellable = system.scheduler.scheduleAtFixedRate(30.minutes, 30.minutes, self, SendPing())

  var listenKey: String = _

  var initialAccountInformation: AccountInformationJson = _

  val restSource: (SourceQueueWithComplete[IncomingBinanceAccountJson], Source[IncomingBinanceAccountJson, NotUsed]) =
    Source.queue[IncomingBinanceAccountJson](1, OverflowStrategy.backpressure).preMaterialize()

  val wsFlow: Flow[Message, IncomingBinanceAccountJson, NotUsed] = Flow.fromFunction {
    case _ => null // TODO
  }

  val downStreamFlow: Flow[IncomingBinanceAccountJson, ExchangeAccountStreamData, NotUsed] = Flow.fromFunction {
    case j:AccountInformationJson => j.toWallet
    case _ => throw new NotImplementedError
  }

  def timestamp:Long = Instant.now.toEpochMilli

  def queryAccountInformation(): AccountInformationJson = {
    import BinanceAccountDataJsonProtocoll._
    Await.result(
      httpRequestJsonBinanceAccount[AccountInformationJson](HttpMethods.GET, s"$BaseRestEndpoint/api/v3/account", s"timestamp=$timestamp", config.secrets),
      Config.httpTimeout.plus(500.millis))
  }


  val SubscribeMessages: List[StreamSubscribeRequestJson] = List()

  def createFlowTo(sink: Sink[ExchangeAccountStreamData, NotUsed]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      wsFlow
        .mergePreferred(restSource._2, priority = true, eagerComplete = false)
        .via(downStreamFlow)
        .toMat(sink)(Keep.right),
      Source(
        SubscribeMessages.map(msg => TextMessage(msg.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/ws/$listenKey")
  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
  var connected: Future[Done.type] = _

  def createConnected: Future[Done.type] =
    ws._1.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        log.debug("WebSocket connected")
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  def createListenKey(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    listenKey = Await.result(
      httpRequestJsonBinanceAccount[ListenKey](HttpMethods.POST, s"$BaseRestEndpoint/api/v3/userDataStream", s"timestamp=$timestamp", config.secrets),
      Config.httpTimeout.plus(500.millis)).listenKey
  }

  def pingUserStream(): Unit = {
    import DefaultJsonProtocol._
    httpRequestJsonBinanceAccount[String](HttpMethods.PUT, s"$BaseRestEndpoint/api/v3/userDataStream?listenKey=$listenKey", s"timestamp=$timestamp", config.secrets)
  }

  override def preStart(): Unit = {
    createListenKey()
  }

  override def receive: Receive = {
    case StartStreamRequest(sink) =>
      log.debug("starting WebSocket stream")
      ws = Http().singleWebSocketRequest(
        WebSocketRequest(WebSocketEndpoint),
        createFlowTo(sink))
      connected = createConnected

      restSource._1.offer(queryAccountInformation())

    case SendPing() => pingUserStream()

  }
}


trait IncomingBinanceAccountJson
case class BalanceJson(asset: String, free: String, locked: String) {
  def toBalance: Balance = Balance(
    Asset(asset),
    free.toDouble,
    locked.toDouble
  )
}
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
                                  permissions: List[String]) extends IncomingBinanceAccountJson {
  def toWallet: ExchangeAccountStreamData =
    Wallet(balances
      .map(_.toBalance)
      .map(e => (e.asset, e))
      .toMap)
}

case class ListenKey(listenKey:String)

object BinanceAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val balanceJson: RootJsonFormat[BalanceJson] = jsonFormat3(BalanceJson)
  implicit val accountInformationJson: RootJsonFormat[AccountInformationJson] = jsonFormat11(AccountInformationJson)
  implicit val listenKeyJson: RootJsonFormat[ListenKey] = jsonFormat1(ListenKey)
}

