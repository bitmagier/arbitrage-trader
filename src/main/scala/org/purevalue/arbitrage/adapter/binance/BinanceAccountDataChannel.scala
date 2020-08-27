package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.HttpUtils.httpRequestJsonBinanceAccount
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.{QueryAccountInformation, SendPing, StartStreamRequest}
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.BaseRestEndpoint
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


object BinanceAccountDataChannel {
  case class StartStreamRequest(sink: Sink[ExchangeAccountStreamData, Future[Done]])
  case class SendPing()
  case class QueryAccountInformation()

  def props(config: ExchangeConfig): Props = Props(new BinanceAccountDataChannel(config))
}

class BinanceAccountDataChannel(config: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceAccountDataChannel])

  private val OutboundAccountInfoStreamName = "outboundAccountInfo"
  private val IdOutboundAccountInfoStream = 1
  private val OutboundAccountPositionStreamName = "outboundAccountPosition"
  private val IdOutboundAccountPositionStream = 2
  private val BalanceUpdateStreamName = "balanceUpdate"
  private val IdBalanceUpdateStream = 3
  private val OrderExecutionReportStreamName = "executionReport"
  private val IdOrderExecutionResportStream = 4

  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val pingSchedule: Cancellable = system.scheduler.scheduleAtFixedRate(30.minutes, 30.minutes, self, SendPing())
  val externalAccountUpdateSchedule: Cancellable = system.scheduler.scheduleAtFixedRate(1.minute, 1.minute, self, QueryAccountInformation())

  var listenKey: String = _

  val restSource: (SourceQueueWithComplete[IncomingBinanceAccountJson], Source[IncomingBinanceAccountJson, NotUsed]) =
    Source.queue[IncomingBinanceAccountJson](1, OverflowStrategy.backpressure).preMaterialize()

  import BinanceAccountDataJsonProtocoll._

  val wsFlow: Flow[Message, Option[IncomingBinanceAccountJson], NotUsed] = Flow.fromFunction {

    case msg: TextMessage =>
      val f: Future[Option[IncomingBinanceAccountJson]] = {
        msg.toStrict(Config.httpTimeout)
          .map(_.getStrictText)
          .map(s => JsonParser(s).asJsObject())
          .map {
            case j: JsObject if j.fields.contains("result") =>
              if (log.isTraceEnabled) log.trace(s"received $j")
              None // ignoring stream subscribe responses
            case j: JsObject if j.fields.contains("e") =>
              if (log.isTraceEnabled) log.trace(s"received $j")
              j.fields("e").convertTo[String] match {
                case OutboundAccountInfoStreamName => Some(j.convertTo[OutboundAccountInfoJson])
                case OutboundAccountPositionStreamName => Some(j.convertTo[OutboundAccountPositionJson])
                case BalanceUpdateStreamName => Some(j.convertTo[BalanceUpdateJson])
                case OrderExecutionReportStreamName => Some(j.convertTo[OrderExecutionReportJson])
                case name: String =>
                  log.error(s"Unknown data stream '$name' received: $j")
                  None
              }
            case j: JsObject =>
              log.warn(s"Unknown json object received: $j")
              None
          }
      }
      try {
        Await.result(f, Config.httpTimeout.plus(1000.millis))
      } catch {
        case e: Exception => throw new RuntimeException(s"While decoding WebSocket stream event: $msg", e)
      }

    case _ =>
      log.warn(s"Received non TextMessage")
      None
  }

  val downStreamFlow: Flow[IncomingBinanceAccountJson, ExchangeAccountStreamData, NotUsed] = Flow.fromFunction {
    case j: AccountInformationJson => j.toWallet
    case j: OutboundAccountPositionJson => j.toWalletUpdate
    case j: BalanceUpdateJson => j.toWalletBalanceUpdate
    // case j: OrderExecutionReportJson => j.toOrderExecutionReport // TODO OrderExecutionReport
    case _ => throw new NotImplementedError
  }

  def queryAccountInformation(): AccountInformationJson = {
    import BinanceAccountDataJsonProtocoll._
    Await.result(
      httpRequestJsonBinanceAccount[AccountInformationJson](HttpMethods.GET, s"$BaseRestEndpoint/api/v3/account", None, config.secrets, sign = true),
      Config.httpTimeout.plus(500.millis))
  }

  val SubscribeMessages: List[AccountStreamSubscribeRequestJson] = List(
    AccountStreamSubscribeRequestJson(params = Seq(OutboundAccountInfoStreamName), id = IdOutboundAccountInfoStream),
    AccountStreamSubscribeRequestJson(params = Seq(OutboundAccountPositionStreamName), id = IdOutboundAccountPositionStream),
    AccountStreamSubscribeRequestJson(params = Seq(BalanceUpdateStreamName), id = IdBalanceUpdateStream),
    AccountStreamSubscribeRequestJson(params = Seq(OrderExecutionReportStreamName), id = IdOrderExecutionResportStream)
  )

  def createFlowTo(sink: Sink[ExchangeAccountStreamData, Future[Done]]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      wsFlow
        .filter(_.isDefined)
        .map(_.get)
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
      httpRequestJsonBinanceAccount[ListenKeyJson](HttpMethods.POST, s"$BaseRestEndpoint/api/v3/userDataStream", None, config.secrets, sign = false),
      Config.httpTimeout.plus(500.millis)).listenKey
    log.debug(s"got listenKey: $listenKey")
  }

  def pingUserStream(): Unit = {
    import DefaultJsonProtocol._
    httpRequestJsonBinanceAccount[String](HttpMethods.PUT,
      s"$BaseRestEndpoint/api/v3/userDataStream?listenKey=$listenKey", None, config.secrets, sign = false)
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

    case QueryAccountInformation() => // do the REST call to get a fresh snapshot. balance changes done via the web interface do not seem to be tracked by the streams
      restSource._1.offer(queryAccountInformation())

    case SendPing() => pingUserStream()
  }
}


case class ListenKeyJson(listenKey: String)
case class AccountStreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

trait IncomingBinanceAccountJson

case class BalanceJson(asset: String, free: String, locked: String) {
  def toBalance: Option[Balance] = {
    if (free.toDouble == 0.0 && locked.toDouble == 0.0)
      None
    else if (!GlobalConfig.AllAssets.keySet.contains(asset))
      throw new Exception(s"We need to ignore a filled balance here, because it's asset is unknown: $this")
    else
      Some(Balance(
        Asset(asset),
        free.toDouble,
        locked.toDouble
      ))
  }
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
  def toWallet: Wallet = Wallet(
    balances
      .flatMap(_.toBalance)
      .map(e => (e.asset, e))
      .toMap)
}

case class OutboundAccountInfoBalanceJson(a: String, f: String, l: String) {
  def toBalance: Option[Balance] = BalanceJson(a, f, l).toBalance
}

case class OutboundAccountInfoJson(e: String, // event type
                                   E: Long, // event time
                                   m: Int, // make commision rate (bips)
                                   t: Int, // taker commission rate (bips)
                                   b: Int, // buyer commission rate (bips)
                                   s: Int, // seller commission rate (bips)
                                   T: Boolean, // can trade?
                                   W: Boolean, // can withdraw?
                                   D: Boolean, // can deposit?
                                   u: Long, // time of last account update
                                   B: List[OutboundAccountInfoBalanceJson], // balances array
                                   P: List[String] // account permissions (e.g. SPOT)
                                  ) extends IncomingBinanceAccountJson

case class OutboundAccountPositionJson(e: String, // event type
                                       E: Long, // event time
                                       u: Long, // time of last account update
                                       B: List[OutboundAccountInfoBalanceJson] // balances
                                      ) extends IncomingBinanceAccountJson {
  def toWalletUpdate: WalletAssetUpdate = WalletAssetUpdate(
    B.flatMap(_.toBalance)
      .map(e => (e.asset, e))
      .toMap)
}

case class BalanceUpdateJson(e: String, // event type
                             E: Long, // event time
                             a: String, // asset
                             d: String, // balance delta
                             T: Long // clear time
                            ) extends IncomingBinanceAccountJson {
  def toWalletBalanceUpdate: WalletBalanceUpdate = {
    if (!GlobalConfig.AllAssets.keySet.contains(a)) throw new Exception(s"We need to ignore a BalanceUpdateJson here, because it's asset is unknown: $this")
    WalletBalanceUpdate(Asset(a), d.toDouble)
  }
}


case class OrderExecutionReportJson( e: String, // Event type
                                     E: Long, // Event time
                                     s: String, // Symbol
                                     c: String, // Client order ID
                                     S: String, // Side ("BUY","SELL")
                                     o: String, // Order type (e.g. "LIMIT")
//                                     f: String, // Time in force e.g. "GTC"
                                     q: String, // Order quantity (e.g. "1.00000000")
                                     p: String, // Order price
                                     P: String, // Stop price
//                                     F: String, // Iceberg quantity
                                     g: Long, // OrderListId
                                     C: String, // Original client order ID; This is the ID of the order being canceled (or null otherwise)
                                     x: String, // Current execution type (e.g. "NEW")
                                     X: String, // Current order status (e.g. "NEW")
                                     r: String, // Order reject reason; will be an error code or "NONE"
                                     i: Long, // Order ID
//                                     l: String, // Last executed quantity
                                     z: String, // Cumulative filled quantity (Average price can be found by doing Z divided by z)
//                                     L: String, // Last executed price
//                                     n: String, // Commission amount
//                                     N: String, // Commission asset (null)
                                     T: Long, // Transaction time
                                     t: Long, // Trade ID
//                                     I: Long, // Ignore
                                     w: Boolean, // Is the order on the book?
                                     m: Boolean, // Is this trade the maker side?
//                                     M: Boolean, // Ignore
                                     O: Long, // Order creation time
                                     Z: String, // Cumulative quote asset transacted quantity
//                                     Y: String, // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
//                                     Q: String // Quote Order Qty
                                   ) extends IncomingBinanceAccountJson

object BinanceAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val listenKeyJson: RootJsonFormat[ListenKeyJson] = jsonFormat1(ListenKeyJson)
  implicit val subscribeMsg: RootJsonFormat[AccountStreamSubscribeRequestJson] = jsonFormat3(AccountStreamSubscribeRequestJson)
  implicit val balanceJson: RootJsonFormat[BalanceJson] = jsonFormat3(BalanceJson)
  implicit val accountInformationJson: RootJsonFormat[AccountInformationJson] = jsonFormat11(AccountInformationJson)
  implicit val outboundAccountInfoBalanceJson: RootJsonFormat[OutboundAccountInfoBalanceJson] = jsonFormat3(OutboundAccountInfoBalanceJson)
  implicit val outboundAccountInfoJson: RootJsonFormat[OutboundAccountInfoJson] = jsonFormat12(OutboundAccountInfoJson)
  implicit val outboundAccountPositionJson: RootJsonFormat[OutboundAccountPositionJson] = jsonFormat4(OutboundAccountPositionJson)
  implicit val balanceUpdateJson: RootJsonFormat[BalanceUpdateJson] = jsonFormat5(BalanceUpdateJson)
  implicit val orderExecutionReportJson: RootJsonFormat[OrderExecutionReportJson] = jsonFormat22(OrderExecutionReportJson)
}
