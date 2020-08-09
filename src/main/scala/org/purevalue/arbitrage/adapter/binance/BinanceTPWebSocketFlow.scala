package org.purevalue.arbitrage.adapter.binance

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinanceDataChannel.{toAsk, toBid}
import org.purevalue.arbitrage.adapter.binance.BinanceTPWebSocketFlow.StartStreamRequest
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.{Await, Future, Promise}

object BinanceTPWebSocketFlow {
  case class StartStreamRequest(sink: Sink[DecodedBinanceMessage, NotUsed])

  def props(config: ExchangeConfig, tradePair: BinanceTradePair, binanceTPDataChannel: ActorRef): Props =
    Props(new BinanceTPWebSocketFlow(config, tradePair, binanceTPDataChannel))
}

case class BinanceTPWebSocketFlow(config: ExchangeConfig, tradePair: BinanceTradePair, binanceTPDataChannel: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTPWebSocketFlow])
  private val symbol = tradePair.symbol.toLowerCase()
  implicit val actorSystem: ActorSystem = Main.actorSystem

  val IdBookTickerStreamRequest: Int = 1
  val IdExtendedTickerStreamRequest: Int = 2
  val IdOrderBookStreamRequest: Int = 3

  val BookTickerStreamName: String = s"$symbol@bookTicker" // realtime
  val ExtendedTickerStreamName: String = s"$symbol@ticker" // update frequency: 1000ms
  val OrderBookStreamName: String = s"$symbol@depth20@100ms"

  import WebSocketJsonProtocoll._
  import actorSystem.dispatcher

  val downStreamFlow: Flow[Message, Option[DecodedBinanceMessage], NotUsed] = Flow.fromFunction {
    case msg: TextMessage =>
      val f: Future[Option[DecodedBinanceMessage]] = {
        msg.toStrict(config.httpTimeout)
          .map(_.getStrictText)
          .map(s => JsonParser(s).asJsObject())
          .map {
            case j: JsObject if j.fields.contains("result") => j.convertTo[StreamSubscribeResponseJson]
            case j: JsObject if j.fields.contains("stream") =>
              j.fields("stream").convertTo[String] match {
                case BookTickerStreamName => j.fields("data").asJsObject.convertTo[RawBookTickerJson]
                case ExtendedTickerStreamName => j.fields("data").asJsObject.convertTo[RawExtendedTickerJson]
                case OrderBookStreamName => j.fields("data").asJsObject.convertTo[RawPartialOrderBookJson]
                case name: String => log.error(s"Unknown data stream '$name' received: $j")
              }
            case j: JsObject => log.error(s"Unknown json object received: $j")
          } map {
          case s: StreamSubscribeResponseJson =>
            if (log.isTraceEnabled) log.trace(s"received $s")
            // if (s.id == IdOrderBookStreamRequest) // TODO inject Orderbook snapshot
              None
          case m: DecodedBinanceMessage =>
            if (log.isTraceEnabled) log.trace(s"received $m")
            Some(m)
          case other =>
            log.warn(s"${Emoji.Confused} Unhandled object (for $tradePair). Message: $other")
            None
        }
      }
      try {
        Await.result(f, config.httpTimeout)
      } catch {
        case e: Exception => throw new RuntimeException(s"While decoding WebSocket stream event: $msg", e)
      }

    case _ =>
      log.warn(s"Received non TextMessage")
      None
  }

  // flow to us
  // emits a list of Messages and then keep the connection open
  def createFlowTo(sink: Sink[DecodedBinanceMessage, NotUsed]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      downStreamFlow
        .filter(_.isDefined)
        .map(_.get)
        .toMat(sink)(Keep.right),
      Source(List(
        TextMessage(StreamSubscribeRequestJson(params = Seq(BookTickerStreamName), id = IdBookTickerStreamRequest).toJson.compactPrint),
        TextMessage(StreamSubscribeRequestJson(params = Seq(ExtendedTickerStreamName), id = IdExtendedTickerStreamRequest).toJson.compactPrint),
        TextMessage(StreamSubscribeRequestJson(params = Seq(OrderBookStreamName), id = IdOrderBookStreamRequest).toJson.compactPrint)
      )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
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
        log.debug("WebSocket connected")
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  // to disconnect call:
  //ws._2.success(None)


  override def receive: Receive = {

    case StartStreamRequest(sink) =>
      log.debug("starting WebSocket stream")
      ws = Http().singleWebSocketRequest(
        WebSocketRequest(WebSocketEndpoint),
        createFlowTo(sink))
      connected = createConnected

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

trait DecodedBinanceMessage
case class StreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)
case class StreamSubscribeResponseJson(result: JsValue, id: Int) extends DecodedBinanceMessage

case class RawPartialOrderBookJson(lastUpdateId: Int, bids: Seq[Seq[String]], asks: Seq[Seq[String]]) extends DecodedBinanceMessage {
  def toOrderBookSnapshot: TPStreamData =
    OrderBookSnapshot(
      bids.map(toBid),
      asks.map(toAsk)
    )
}
//case class RawOrderBookUpdateJson(e: String /* depthUpdate */ , E: Long /* event time */ , s: String /* symbol */ ,
//                                  U: Long /* first update ID in event */ ,
//                                  u: Long /* final update ID in event */ ,
//                                  b: Seq[Seq[String]],
//                                  a: Seq[Seq[String]]) extends DecodedBinanceMessage {
//  def toOrderBookUpdate: OrderBookUpdate = {
//    OrderBookUpdate(
//      b.map(toBidUpdate),
//      a.map(toAskUpdate)
//    )
//  }
//}

// {"e":"24hrTicker","E":1596735092288,"s":"ADABTC","p":"-0.00000008","P":"-0.651","w":"0.00001214","x":"0.00001228","c":"0.00001220","Q":"5329.00000000",
//  "b":"0.00001220","B":"10709.00000000","a":"0.00001221","A":"323762.00000000","o":"0.00001228","h":"0.00001239","l":"0.00001196","v":"147269686.00000000",
//  "q":"1788.50464895","O":1596648691106,"C":1596735091106,"F":39864151,"L":39900689,"n":36539}
case class RawExtendedTickerJson(e: String, // e == "24hrTicker"
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
                                 n: Long // total number of trades
                                ) extends DecodedBinanceMessage {
  def toExtendedTicker(exchange: String, tradePair: TradePair): ExtendedTicker =
    ExtendedTicker(exchange, tradePair, b.toDouble, B.toDouble, a.toDouble, A.toDouble, c.toDouble, Q.toDouble, w.toDouble, LocalDateTime.now)
}

case class RawBookTickerJson(u: Long, // order book updateId
                             s: String, // symbol
                             b: String, // best bid price
                             B: String, // best bid quantity
                             a: String, // best ask price
                             A: String // best ask quantity
                            ) extends DecodedBinanceMessage {
  def toTicker(exchange: String, tradePair: TradePair): Ticker =
    Ticker(exchange, tradePair, b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), None, LocalDateTime.now)
}

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequestJson] = jsonFormat3(StreamSubscribeRequestJson)
  implicit val subscribeResponseMsg: RootJsonFormat[StreamSubscribeResponseJson] = jsonFormat2(StreamSubscribeResponseJson)
  //  implicit val bookUpdate: RootJsonFormat[RawOrderBookUpdateJson] = jsonFormat7(RawOrderBookUpdateJson)
  implicit val partialBook: RootJsonFormat[RawPartialOrderBookJson] = jsonFormat3(RawPartialOrderBookJson)
  implicit val rawBookTicker: RootJsonFormat[RawBookTickerJson] = jsonFormat6(RawBookTickerJson)
  implicit val rawExtendedTicker: RootJsonFormat[RawExtendedTickerJson] = jsonFormat22(RawExtendedTickerJson)
}
