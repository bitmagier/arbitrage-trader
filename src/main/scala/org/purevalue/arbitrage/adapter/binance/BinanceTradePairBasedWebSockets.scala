package org.purevalue.arbitrage.adapter.binance

import java.time.LocalDateTime

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.scaladsl._
import org.purevalue.arbitrage._
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.{Future, Promise}

object BinanceTradePairBasedWebSockets {
  def props(config: ExchangeConfig, tradePair: BinanceTradePair, tradePairDataStreamer: ActorRef): Props =
    Props(new BinanceTradePairBasedWebSockets(config, tradePair, tradePairDataStreamer))
}

case class BinanceTradePairBasedWebSockets(config: ExchangeConfig, tradePair: BinanceTradePair, tradePairDataStreamer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTradePairBasedWebSockets])
  private val symbol = tradePair.symbol.toLowerCase()
  implicit val actorSystem: ActorSystem = Main.actorSystem

  import WebSocketJsonProtocoll._
  import actorSystem.dispatcher

  private def handleSubscribeResponse(msg: SubscribeResponse): Unit = {
    if (log.isTraceEnabled) log.trace(s"received SubscribeResponse message: $msg")
  }

  private def handleDepthUpdate(u: RawOrderBookUpdate): Unit = {
    if (log.isTraceEnabled) log.trace(s"received OrderBook update: $u")
    u match {
      case u if u.s == tradePair.symbol => tradePairDataStreamer ! u
      case x@_ => log.warn(s"${Emoji.Confused} RawOrderBookUpdate contains wrong TradePair. Expected was '$tradePair'. Message was: $x")
    }
  }

  private def handleMiniTicker(t: RawTicker): Unit = {
    if (log.isTraceEnabled) log.trace(s"received MiniTicker: $t")
    t match {
      case t if t.s == tradePair.symbol => tradePairDataStreamer ! t
      case x@_ => log.warn(s"${Emoji.Confused} RawMiniTicker contains wrong TradePair. Expected was '$tradePair'. Message was: $x`")
    }
  }

  val sink: Sink[Message, Future[Done]] = Sink.foreach[Message] {
    case msg: TextMessage =>
      msg.toStrict(config.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject())
        .map {
          case j if j.fields.contains("result") => j.convertTo[SubscribeResponse]
          case j if j.fields.contains("e") && j.fields("e").convertTo[String] == "depthUpdate" => j.convertTo[RawOrderBookUpdate]
          case j if j.fields.contains("e") && j.fields("e").convertTo[String] == "24hrMiniTicker" => j.convertTo[RawTicker]
          case x: JsObject => log.error(s"Unknown json message received: $x"); x
        } map {
        case m: SubscribeResponse => handleSubscribeResponse(m)
        case m: RawOrderBookUpdate => handleDepthUpdate(m)
        case m: RawTicker => handleMiniTicker(m)
      }
    case x@_ => log.warn(s"Received non TextMessage: $x")
  }

  // flow to us
  // emits a Message and then keep the connection open
  val flow: Flow[Message, Message, Promise[Option[Message]]] =
  Flow.fromSinkAndSourceCoupledMat(
    sink,
    Source(List(
      TextMessage(StreamSubscribeRequest(params = Seq(s"$symbol@depth"), id = 1).toJson.compactPrint),
      TextMessage(StreamSubscribeRequest(params = Seq(s"$symbol@miniTicker"), id = 2).toJson.compactPrint)
    )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)


  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/ws/$symbol@depth") // TODO add "@100ms" and solve akka.stream.BufferOverflowException: Exceeded configured max-open-requests value of [32]
  val (upgradeResponse, promise) =
    Http().singleWebSocketRequest(
      WebSocketRequest(WebSocketEndpoint),
      flow)

  // just like a regular http request we can access response status which is available via upgrade.response.status
  // status code 101 (Switching Protocols) indicates that server support WebSockets
  val connected: Future[Done.type] = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      log.debug("WebSocket connected")
      Future.successful(Done)
    } else {
      throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
  }

  // to disconnect call:
  //promise.success(None)

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

case class StreamSubscribeRequest(method: String = "SUBSCRIBE", params: Seq[String], id: Int)
case class SubscribeResponse(result: JsValue, id: Int)
case class RawOrderBookUpdate(e: String /* depthUpdate */ , E: Long /* event time */ , s: String /* symbol */ ,
                              U: Long /* first update ID in event */ ,
                              u: Long /* final update ID in event */ ,
                              b: Seq[Seq[String]],
                              a: Seq[Seq[String]])

case class RawTicker(e: String, // e == "24hrTicker"
                     E: Long, // event time
                     s: String, // symbol (e.g. BNBBTC)
                     p: Double, // price change
                     P: Double, // price change percent
                     w: Double, // weighted average price
//                     x: Double, // First trade(F)-1 price (first trade before the 24hr rolling window)
                     c: Double, // last price
                     Q: Double, // last quantity
                     b: Double, // best bid price
                     B: Double, // best bid quantity
                     a: Double, // best ask price
                     A: Double, // best ask quantity
                     o: Double, // open price
                     h: Double, // high price
                     l: Double, // low price
                     v: Double, // total traded base asset volume
                     q: Double, // total traded quote asset volume
                     O: Long, // statistics open time
                     C: Long, // statistics close time
                     F: Long, // first trade ID
                     L: Long, // last trade ID
                     n: Long) { // total number of trades
  def toTicker(exchange:String, tradePair:TradePair): Ticker = Ticker(exchange, tradePair, b, Some(B), a, Some(A), c, Some(Q), Some(w), LocalDateTime.now)
}

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequest] = jsonFormat3(StreamSubscribeRequest)
  implicit val subscribeResponseMsg: RootJsonFormat[SubscribeResponse] = jsonFormat2(SubscribeResponse)
  implicit val bookUpdate: RootJsonFormat[RawOrderBookUpdate] = jsonFormat7(RawOrderBookUpdate)
  implicit val rawTicker: RootJsonFormat[RawTicker] = jsonFormat22(RawTicker)
}

// TODO handle temporary down trading pair - at init time - where no subscribe response is deliverd, as well as during trading time (event?)
