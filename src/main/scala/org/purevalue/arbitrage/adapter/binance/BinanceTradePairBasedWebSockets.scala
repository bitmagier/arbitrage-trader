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
import scala.util.{Failure, Success}

object BinanceTradePairBasedWebSockets {
  def props(config: ExchangeConfig, tradePair: BinanceTradePair, tradePairDataStreamer: ActorRef): Props =
    Props(new BinanceTradePairBasedWebSockets(config, tradePair, tradePairDataStreamer))
}

case class BinanceTradePairBasedWebSockets(config: ExchangeConfig, tradePair: BinanceTradePair, tradePairDataStreamer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTradePairBasedWebSockets])
  private val symbol = tradePair.symbol.toLowerCase()
  implicit val actorSystem: ActorSystem = Main.actorSystem

  val TickerStreamName: String = s"$symbol@ticker"
  val OrderBookStreamName: String = s"$symbol@depth"

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

  private def handleTicker(t: RawTicker): Unit = {
    if (log.isTraceEnabled) log.trace(s"received Ticker: $t")
    t.s match {
      case tradePair.symbol => tradePairDataStreamer ! t
      case x@_ => log.warn(s"${Emoji.Confused} RawTicker contains wrong TradePair. Expected was '$tradePair'. Message was: $x`")
    }
  }

  val sink: Sink[Message, Future[Done]] = Sink.foreach[Message] {
    case msg: TextMessage =>
      msg.toStrict(config.httpTimeout)
        .map(_.getStrictText)
        //        .map(s => {
        //          if (log.isTraceEnabled) log.trace(s)
        //          s
        //        })
        .map(s => JsonParser(s).asJsObject())
        .map {
          case j: JsObject if j.fields.contains("result") => j.convertTo[SubscribeResponse]
          case j: JsObject if j.fields.contains("stream") =>
            j.fields("stream").convertTo[String] match {
              case TickerStreamName => j.fields("data").asJsObject.convertTo[RawTicker]
              case OrderBookStreamName => j.fields("data").asJsObject.convertTo[RawOrderBookUpdate]
              case name: String => log.error(s"Unknown data stream '$name' received: $j")
            }
          case j: JsObject => log.error(s"Unknown json object received: $j")
        } onComplete {
        case Success(v) => v match {
          case m: SubscribeResponse => handleSubscribeResponse(m)
          case r: RawTicker => handleTicker(r)
          case o: RawOrderBookUpdate => handleDepthUpdate(o)
          case _ => log.error("Unhandled object")
        }
        case Failure(e) => log.error("Error while decoding message: ", e)
      }
    case _ => log.warn(s"Received non TextMessage")
  }

  // flow to us
  // emits a Message and then keep the connection open
  val flow: Flow[Message, Message, Promise[Option[Message]]] =
  Flow.fromSinkAndSourceCoupledMat(
    sink,
    Source(List(
      TextMessage(StreamSubscribeRequest(params = Seq(TickerStreamName), id = 1).toJson.compactPrint),
      TextMessage(StreamSubscribeRequest(params = Seq(OrderBookStreamName), id = 2).toJson.compactPrint)
    )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)


  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/stream") // TODO add "@100ms" and solve akka.stream.BufferOverflowException: Exceeded configured max-open-requests value of [32]
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

// {"e":"24hrTicker","E":1596735092288,"s":"ADABTC","p":"-0.00000008","P":"-0.651","w":"0.00001214","x":"0.00001228","c":"0.00001220","Q":"5329.00000000",
//  "b":"0.00001220","B":"10709.00000000","a":"0.00001221","A":"323762.00000000","o":"0.00001228","h":"0.00001239","l":"0.00001196","v":"147269686.00000000",
//  "q":"1788.50464895","O":1596648691106,"C":1596735091106,"F":39864151,"L":39900689,"n":36539}
case class RawTicker(e: String, // e == "24hrTicker"
                     E: Long, // event time
                     s: String, // symbol (e.g. BNBBTC)
                     p: String, // price change
                     P: String, // price change percent
                     w: String, // weighted average price
                     //                     x: Double, // First trade(F)-1 price (first trade before the 24hr rolling window)
                     c: String, // last price
                     Q: String, // last quantity
                     b: String, // best bid price
                     B: String, // best bid quantity
                     a: String, // best ask price
                     A: String, // best ask quantity
                     o: String, // open price
                     h: String, // high price
                     l: String, // low price
                     v: String, // total traded base asset volume
                     q: String, // total traded quote asset volume
                     O: Long, // statistics open time
                     C: Long, // statistics close time
                     F: Long, // first trade ID
                     L: Long, // last trade ID
                     n: Long) { // total number of trades
  def toTicker(exchange: String, tradePair: TradePair): Ticker =
    Ticker(exchange, tradePair, b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), c.toDouble, Some(Q.toDouble), Some(w.toDouble), LocalDateTime.now)
}

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequest] = jsonFormat3(StreamSubscribeRequest)
  implicit val subscribeResponseMsg: RootJsonFormat[SubscribeResponse] = jsonFormat2(SubscribeResponse)
  implicit val bookUpdate: RootJsonFormat[RawOrderBookUpdate] = jsonFormat7(RawOrderBookUpdate)
  implicit val rawTicker: RootJsonFormat[RawTicker] = jsonFormat22(RawTicker)
}

// TODO handle temporary down trading pair - at init time - where no subscribe response is deliverd, as well as during trading time (event?)
