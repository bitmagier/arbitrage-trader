//package org.purevalue.arbitrage.adapter.bitfinex
//
//import java.time.Instant
//
//import akka.{Done, NotUsed}
//import akka.actor.{Actor, ActorRef, ActorSystem, Props}
//import akka.http.scaladsl.Http
//import akka.http.scaladsl.model.{StatusCodes, Uri}
//import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
//import akka.stream.OverflowStrategy
//import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
//import org.purevalue.arbitrage.ExchangeAccountDataManager.{CancelOrder, FetchOrder, NewLimitOrder}
//import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.StartStreamRequest
//import org.purevalue.arbitrage.adapter.binance.TPStreamSubscribeRequestJson
//import org.purevalue.arbitrage.adapter.binance.WebSocketJsonProtocoll.jsonFormat3
//import org.purevalue.arbitrage.{Config, ExchangeAccountStreamData, ExchangeConfig, HttpUtils, Main}
//import org.slf4j.LoggerFactory
//import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}
//
//import scala.concurrent.duration.DurationInt
//import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
//
//
//object BitfinexAccountDataChannel {
//  case class StartStreamRequest(sink: Sink[ExchangeAccountStreamData, Future[Done]])
//
//  def props(config: ExchangeConfig, exchangePublicDataInquirer: ActorRef): Props = Props(new BitfinexAccountDataChannel(config, exchangePublicDataInquirer))
//}
//class BitfinexAccountDataChannel(config: ExchangeConfig, exchangePublicDataInquirer: ActorRef) extends Actor {
//  private val log = LoggerFactory.getLogger(classOf[BitfinexAccountDataChannel])
//
//  implicit val system: ActorSystem = Main.actorSystem
//  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//
//  val restSource: (SourceQueueWithComplete[IncomingBitfinexAccountJson], Source[IncomingBitfinexAccountJson, NotUsed]) =
//    Source.queue[IncomingBitfinexAccountJson](10, OverflowStrategy.backpressure).preMaterialize()
//
//  import BitfinexAccountDataJsonProtocoll._
//
//  val wsFlow: Flow[Message, Option[IncomingBitfinexAccountJson], NotUsed] = Flow.fromFunction {
//
//    case msg: TextMessage =>
//      val f: Future[Option[IncomingBitfinexAccountJson]] = {
//        msg.toStrict(Config.httpTimeout)
//          .map(_.getStrictText)
//          .map(s => JsonParser(s).asJsObject())
//          .map {
//            case ???
//            case j: JsObject =>
//              log.warn(s"Unknown json object received: $j")
//              None
//          }
//      }
//      try {
//        Await.result(f, Config.httpTimeout.plus(1000.millis))
//      } catch {
//        case e: Exception => throw new RuntimeException(s"While decoding WebSocket stream event: $msg", e)
//      }
//
//    case _ =>
//      log.warn(s"Received non TextMessage")
//      None
//  }
//
//  val downStreamFlow: Flow[IncomingBitfinexAccountJson, ExchangeAccountStreamData, NotUsed] = Flow.fromFunction {
//
//    case x@_ => log.debug(s"$x"); throw new NotImplementedError
//    // @formatter:on
//  }
//
//  def authMessage: BinanceAuthMessage = {
//    val authNonce: String = Instant.now.toEpochMilli.toString
//    val authPayload = "AUTH" + authNonce
//    BinanceAuthMessage(
//      apiKey = config.secrets.apiKey,
//      HttpUtils.hmacSha384Signature(authPayload, config.secrets.apiSecretKey),
//      authNonce,
//      authPayload)
//  }
//
//  def createFlowTo(sink: Sink[ExchangeAccountStreamData, Future[Done]]): Flow[Message, Message, Promise[Option[Message]]] = {
//    Flow.fromSinkAndSourceCoupledMat(
//      wsFlow
//        .filter(_.isDefined)
//        .map(_.get)
//        .mergePreferred(restSource._2, priority = true, eagerComplete = false)
//        .via(downStreamFlow)
//        .toMat(sink)(Keep.right),
//      Source(
//        List(TextMessage(authMessage.toJson.compactPrint))
//      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
//  }
//
//  def buildRequestParams: String =
//    s"""event=auth
//       |apiKey=${config.secrets.apiKey}
//       |authPayload=
//       |authSig=
//       |authNonce
//       |""".stripMargin
//
//  val WebSocketEndpoint: Uri = Uri(s"wss://api.bitfinex.com/ws/2")
//  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
//  var connected: Future[Done.type] = _
//
//  def createConnected: Future[Done.type] =
//    ws._1.flatMap { upgrade =>
//      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
//        if (log.isTraceEnabled) log.trace("WebSocket connected")
//        Future.successful(Done)
//      } else {
//        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
//      }
//    }
//
//  override def receive: Receive = {
//    case StartStreamRequest(sink) =>
//      log.trace("starting WebSocket stream")
//      ws = Http().singleWebSocketRequest(
//        WebSocketRequest(WebSocketEndpoint),
//        createFlowTo(sink))
//      connected = createConnected
//
//      restSource._1.offer(queryAccountInformation())
//      queryOpenOrders().foreach(e => restSource._1.offer(e))
//
//    // Messages from ExchangeAccountDataManager (forwarded from TradeRoom-LiquidityManager or TradeRoom-OrderExecutionManager)
//    case CancelOrder(tradePair, externalOrderId) =>
//      cancelOrder(tradePair, externalOrderId.toLong).pipeTo(sender())
//
//    case NewLimitOrder(o) =>
//      newLimitOrder(o).pipeTo(sender())
//
//    // TODO currently unused: query unfinished persisted orders from TradeRoom after application restart
//    case FetchOrder(tradePair, externalOrderId) => ???
//    //      restSource._1.offer(queryOrder(resolveSymbol(tradePair), externalOrderId.toLong))
//  }
//}
//
//trait IncomingBitfinexAccountJson
//
//case class BinanceAuthMessage(apiKey:String, authSig:String, authNonce:String, authPayload:String, event:String = "auth")
//
//object BitfinexAccountDataJsonProtocoll extends DefaultJsonProtocol {
//  implicit val binanceAuthMessage: RootJsonFormat[BinanceAuthMessage] = jsonFormat5(BinanceAuthMessage)
//}