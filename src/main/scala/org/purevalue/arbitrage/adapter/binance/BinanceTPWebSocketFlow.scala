package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.{toAsk, toBid}
import org.purevalue.arbitrage.adapter.binance.BinanceTPWebSocketFlow.StartStreamRequest
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}


object BinanceTPWebSocketFlow {
  case class StartStreamRequest(sink: Sink[IncomingBinanceTradepairJson, NotUsed])

  def props(config: ExchangeConfig, tradePair: BinanceTradePair, binanceTPDataChannel: ActorRef): Props =
    Props(new BinanceTPWebSocketFlow(config, tradePair, binanceTPDataChannel))
}

case class BinanceTPWebSocketFlow(config: ExchangeConfig, tradePair: BinanceTradePair, binanceTPDataChannel: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTPWebSocketFlow])
  private val symbol = tradePair.symbol.toLowerCase()
  implicit val actorSystem: ActorSystem = Main.actorSystem

  val BaseRestEndpoint = "https://api.binance.com"

  val IdBookTickerStreamRequest: Int = 1
  val IdExtendedTickerStreamRequest: Int = 2
  val IdOrderBookStreamRequest: Int = 3

  val BookTickerStreamName: String = s"$symbol@bookTicker" // realtime
  val OrderBookStreamName: String = s"$symbol@depth20@100ms"

  import WebSocketJsonProtocoll._
  import actorSystem.dispatcher

  val downStreamWSFlow: Flow[Message, Option[IncomingBinanceTradepairJson], NotUsed] = Flow.fromFunction {
    case msg: TextMessage =>
      val f: Future[Option[IncomingBinanceTradepairJson]] = {
        msg.toStrict(Config.httpTimeout)
          .map(_.getStrictText)
          .map(s => JsonParser(s).asJsObject())
          .map {
            case j: JsObject if j.fields.contains("result") =>
              if (log.isTraceEnabled) log.trace(s"received $j")
              None // ignoring stream subscribe responses
            case j: JsObject if j.fields.contains("stream") =>
              j.fields("stream").convertTo[String] match {
                case BookTickerStreamName =>
                  Some(j.fields("data").asJsObject.convertTo[RawBookTickerStreamJson])
                case OrderBookStreamName =>
                  Some(j.fields("data").asJsObject.convertTo[RawPartialOrderBookStreamJson])
                case name: String =>
                  log.warn(s"${Emoji.Confused}  Unhandled data stream $name received: $j")
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

  val DefaultSubscribeMessages: List[TPStreamSubscribeRequestJson] = List(
    TPStreamSubscribeRequestJson(params = Seq(BookTickerStreamName), id = IdBookTickerStreamRequest),
  )
  val SubscribeMessages: List[TPStreamSubscribeRequestJson] = if (config.orderBooksEnabled)
    TPStreamSubscribeRequestJson(params = Seq(OrderBookStreamName), id = IdOrderBookStreamRequest) :: DefaultSubscribeMessages
  else DefaultSubscribeMessages

  val restSource: (SourceQueueWithComplete[IncomingBinanceTradepairJson], Source[IncomingBinanceTradepairJson, NotUsed]) =
    Source.queue[IncomingBinanceTradepairJson](1, OverflowStrategy.backpressure).preMaterialize()

  // flow to us
  // emits a list of Messages and then keep the connection open
  def createFlowTo(sink: Sink[IncomingBinanceTradepairJson, NotUsed]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      downStreamWSFlow
        .filter(_.isDefined)
        .map(_.get)
        .mergePreferred(restSource._2, priority = true, eagerComplete = false) // merge with data coming from REST requests (preferring REST data)
        .toMat(sink)(Keep.right),
      Source(
        SubscribeMessages.map(msg => TextMessage(msg.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }


  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/stream")
  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _

  // just like a regular http request we can access response status which is available via upgrade.response.status
  // status code 101 (Switching Protocols) indicates that server support WebSockets
  var connected: Future[Done.type] = _

  def createConnected: Future[Done.type] =
    ws._1.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        if (log.isTraceEnabled) log.trace("WebSocket connected")
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  // to disconnect call:
  //ws._2.success(None)


  def deliverBookTickerState(): Unit = {
    httpGetJson[RawBookTickerRestJson](s"$BaseRestEndpoint/api/v3/ticker/bookTicker?symbol=${tradePair.symbol}") onComplete {
      case Success(ticker) =>
        restSource._1.offer(ticker)
      case Failure(e) =>
        log.error("Query/Transform RawBookTickerRestJson failed", e)
    }
  }

  override def receive: Receive = {

    case StartStreamRequest(sink) =>
      log.trace("starting WebSocket stream")
      ws = Http().singleWebSocketRequest(
        WebSocketRequest(WebSocketEndpoint),
        createFlowTo(sink))
      connected = createConnected
      deliverBookTickerState()

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

case class TPStreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

trait IncomingBinanceTradepairJson

case class RawPartialOrderBookStreamJson(lastUpdateId: Int, bids: Seq[Seq[String]], asks: Seq[Seq[String]]) extends IncomingBinanceTradepairJson {
  def toOrderBookSnapshot: ExchangeTPStreamData =
    OrderBookSnapshot(
      bids.map(toBid),
      asks.map(toAsk)
    )
}

case class RawBookTickerRestJson(symbol: String,
                                 bidPrice: String,
                                 bidQty: String,
                                 askPrice: String,
                                 askQty: String) extends IncomingBinanceTradepairJson {
  def toTicker(exchange: String, tradePair: TradePair): Ticker = {
    Ticker(exchange, tradePair, bidPrice.toDouble, Some(bidQty.toDouble), askPrice.toDouble, Some(askQty.toDouble), None)
  }
}

case class RawBookTickerStreamJson(u: Long, // order book updateId
                                   s: String, // symbol
                                   b: String, // best bid price
                                   B: String, // best bid quantity
                                   a: String, // best ask price
                                   A: String // best ask quantity
                                  ) extends IncomingBinanceTradepairJson {
  def toTicker(exchange: String, tradePair: TradePair): Ticker =
    Ticker(exchange, tradePair, b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), None)
}

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[TPStreamSubscribeRequestJson] = jsonFormat3(TPStreamSubscribeRequestJson)
  //  implicit val bookUpdate: RootJsonFormat[RawOrderBookUpdateJson] = jsonFormat7(RawOrderBookUpdateJson)
  implicit val partialBookStream: RootJsonFormat[RawPartialOrderBookStreamJson] = jsonFormat3(RawPartialOrderBookStreamJson)
  implicit val rawBookTickerRest: RootJsonFormat[RawBookTickerRestJson] = jsonFormat5(RawBookTickerRestJson)
  implicit val rawBookTickerStream: RootJsonFormat[RawBookTickerStreamJson] = jsonFormat6(RawBookTickerStreamJson)
}
