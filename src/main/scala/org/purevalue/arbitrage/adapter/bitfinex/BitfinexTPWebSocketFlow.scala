package org.purevalue.arbitrage.adapter.bitfinex

import java.time.LocalDateTime

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.scaladsl._
import org.purevalue.arbitrage._
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}


trait DecodedMessage
case class UnknownDataMessage(m: String) extends DecodedMessage
case class JsonMessage(j: JsObject) extends DecodedMessage
case class SubscribeRequest(event: String = "subscribe", channel: String, symbol: String)

case class Heartbeat() extends DecodedMessage

case class RawOrderBookEntry(price: Double, count: Int, amount: Double)
object RawOrderBookEntry {
  def apply(v: Tuple3[Double, Int, Double]): RawOrderBookEntry = RawOrderBookEntry(v._1, v._2, v._3)
}

case class RawOrderBookSnapshotMessage(channelId: Int, values: List[RawOrderBookEntry]) extends DecodedMessage { // [channelId, [[price, count, amount],...]]
  private def toOrderBookSnapshot(snapshot: RawOrderBookSnapshotMessage) = {
    val bids = snapshot.values
      .filter(_.count > 0)
      .filter(_.amount > 0)
      .map(e => Bid(e.price, e.amount))
    val asks = snapshot.values
      .filter(_.count > 0)
      .filter(_.amount < 0)
      .map(e => Ask(e.price, -e.amount))
    OrderBookSnapshot(
      bids, asks
    )
  }
}
object RawOrderBookSnapshotMessage {
  def apply(v: Tuple2[Int, List[RawOrderBookEntry]]): RawOrderBookSnapshotMessage = RawOrderBookSnapshotMessage(v._1, v._2)
}

case class RawOrderBookUpdateMessage(channelId: Int, value: RawOrderBookEntry) extends DecodedMessage // [channelId, [price, count, amount]]
object RawOrderBookUpdateMessage {
  def apply(v: Tuple2[Int, RawOrderBookEntry]): RawOrderBookUpdateMessage = RawOrderBookUpdateMessage(v._1, v._2)
}

case class RawTicker(bid: Double, // Price of last highest bid
                     bidSize: Double, // Sum of 25 highest bid sizes
                     ask: Double, // Price of last lowest ask
                     askSize: Double, // Sum of 25 lowest ask sizes
                     dailyChange: Double, // Amount that the last price has changed since yesterday
                     dailyChangeRelative: Double, // Relative price change since yesterday (*100 for percentage change)
                     lastPrice: Double, // Price of the last trade
                     volume: Double, // Daily volume
                     high: Double, // Daily high
                     low: Double) { // Daily low
  def toTicker(exchange: String, tradePair: TradePair): Ticker =
    Ticker(exchange, tradePair, bid, None, ask, None, Some(lastPrice), LocalDateTime.now)
}
object RawTicker {
  def apply(v: Array[Double]): RawTicker =
    RawTicker(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9))
}

case class RawTickerMessage(channelId: Int, value: RawTicker) extends DecodedMessage // [channelId, [bid, bidSize, ask, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low]]

object RawTickerMessage {
  def apply(v: Tuple2[Int, RawTicker]): RawTickerMessage = RawTickerMessage(v._1, v._2)
}


object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeRequest: RootJsonFormat[SubscribeRequest] = jsonFormat3(SubscribeRequest)

  implicit object rawOrderBookEntryFormat extends RootJsonFormat[RawOrderBookEntry] {
    def read(value: JsValue): RawOrderBookEntry = RawOrderBookEntry(value.convertTo[Tuple3[Double, Int, Double]])

    def write(v: RawOrderBookEntry): JsValue = ???
  }

  implicit object rawOrderBookSnapshotFormat extends RootJsonFormat[RawOrderBookSnapshotMessage] {
    def read(value: JsValue): RawOrderBookSnapshotMessage = RawOrderBookSnapshotMessage(value.convertTo[Tuple2[Int, List[RawOrderBookEntry]]])

    def write(v: RawOrderBookSnapshotMessage): JsValue = ???
  }

  implicit object rawOrderBookUpdateFormat extends RootJsonFormat[RawOrderBookUpdateMessage] {
    def read(value: JsValue): RawOrderBookUpdateMessage = RawOrderBookUpdateMessage(value.convertTo[Tuple2[Int, RawOrderBookEntry]])

    def write(v: RawOrderBookUpdateMessage): JsValue = ???
  }

  implicit object rawTickerFormar extends RootJsonFormat[RawTicker] {
    def read(value: JsValue): RawTicker = RawTicker(value.convertTo[Array[Double]])

    def write(v: RawTicker): JsValue = ???
  }

  implicit object rawTickerMessageFormat extends RootJsonFormat[RawTickerMessage] {
    def read(value: JsValue): RawTickerMessage = RawTickerMessage(value.convertTo[Tuple2[Int, RawTicker]])

    def write(v: RawTickerMessage): JsValue = ???
  }
}


object BitfinexTradePairBasedWebSockets {
  def props(config: ExchangeConfig, tradePair: BitfinexTradePair, tradePairDataStreamer: ActorRef): Props =
    Props(new BitfinexTradePairBasedWebSockets(config, tradePair, tradePairDataStreamer))
}

case class BitfinexTradePairBasedWebSockets(config: ExchangeConfig, tradePair: BitfinexTradePair, tradePairDataStreamer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexTradePairBasedWebSockets])

  implicit val actorSystem: ActorSystem = Main.actorSystem

  import WebSocketJsonProtocoll._
  import actorSystem.dispatcher

  var tickerChannelId: Int = _
  var orderBookChannelId: Int = _


  // TODO handle bitfinex Info codes:
  // 20051 : Stop/Restart Websocket Server (please reconnect)
  // 20060 : Entering in Maintenance mode. Please pause any activity and resume after receiving the info message 20061 (it should take 120 seconds at most).
  // 20061 : Maintenance ended. You can resume normal activity. It is advised to unsubscribe/subscribe again all channels.

  def handleEvent(event: String, j: JsObject): Unit = event match {
    case "subscribed" =>
      if (log.isTraceEnabled) log.trace(s"received SubscribeResponse message: $j")
      val channel = j.fields("channel").convertTo[String]
      val channelId = j.fields("chanId").convertTo[Int]
      channel match {
        case "ticker" => tickerChannelId = channelId; log.debug(s"$tradePair: ticker channelId=$channelId")
        case "book" => orderBookChannelId = channelId; log.debug(s"$tradePair: book channelId=$channelId")
      }
    case "error" => log.error(s"received error message: $j")
    case "info" => log.debug(s"received info message: $j")
    case _ => log.warn(s"received unidentified message: $j")
  }

  def decodeJson(s: String): DecodedMessage = JsonMessage(JsonParser(s).asJsObject)

  def decodeDataArray(s: String): DecodedMessage = {
    val heatBeatPattern = """^\[\s*\d+,\s*"hb"\s*]""".r
    heatBeatPattern.findFirstIn(s) match {
      case Some(_) => Heartbeat()
      case None =>
        val channelIdDecodePatter = """^\[(\d+)\s*,.*""".r
        channelIdDecodePatter.findFirstMatchIn(s) match {
          case Some(m) =>
            val TickerChannelId = tickerChannelId
            val OrderBookChannelId = orderBookChannelId
            m.group(1).toInt match {
              case TickerChannelId => JsonParser(s).convertTo[RawTickerMessage]
              case OrderBookChannelId =>
                val snapshotPattern = "^\\[\\d+\\s*,\\s*\\[\\s*\\[.*".r
                snapshotPattern.findFirstIn(s) match {
                  case Some(_) => JsonParser(s).convertTo[RawOrderBookSnapshotMessage]
                  case None => JsonParser(s).convertTo[RawOrderBookUpdateMessage]
                }
              case id@_ =>
                log.error(s"${Emoji.SadAndConfused} [bitfinex:$tradePair] data message with unknown channelId $id received.")
                UnknownDataMessage(s)
            }
          case None => throw new RuntimeException(s"${Emoji.SadAndConfused} Unable to decode bifinex data message:\n$s")
        }
    }
  }

  val sink: Sink[Message, Future[Done]] = Sink.foreach[Message] {
    case msg: TextMessage =>
      msg.toStrict(config.httpTimeout)
        .map(_.getStrictText)
        .map {
          case s: String if s.startsWith("{") => decodeJson(s)
          case s: String if s.startsWith("[") => decodeDataArray(s)
        }.onComplete {
        case Failure(exception) => log.error(s"Unable to decode expected Json message", exception)
        case Success(v) => v match {
          case h: Heartbeat => if (log.isTraceEnabled) log.trace(s"heartbeat received: $h")
          case j: JsonMessage if j.j.fields.contains("event") => handleEvent(j.j.fields("event").convertTo[String], j.j)
          case j: JsonMessage => log.warn(s"Unhandled JsonMessage received: $j")
          case t: RawTickerMessage => tradePairDataStreamer ! t
          case d: RawOrderBookSnapshotMessage => tradePairDataStreamer ! d
          case d: RawOrderBookUpdateMessage => tradePairDataStreamer ! d
          case x@_ => log.error(s"Unhandled message: $x")
        }
      }
    case msg: Message =>
      log.warn(s"Unexpected kind of Message received: $msg")
  }


  // flow to us
  // emits a Message and then keep the connection open
  val flow: Flow[Message, Message, Promise[Option[Message]]] =
  Flow.fromSinkAndSourceCoupledMat(
    sink,
    Source(List(
      TextMessage(SubscribeRequest(channel = "ticker", symbol = tradePair.apiSymbol).toJson.compactPrint),
      TextMessage(SubscribeRequest(channel = "book", symbol = tradePair.apiSymbol).toJson.compactPrint)
    )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)


  val WebSocketEndpoint: Uri = Uri(s"wss://api-pub.bitfinex.com/ws/2")
  val (upgradeResponse, promise) =
    Http().singleWebSocketRequest(
      WebSocketRequest(WebSocketEndpoint),
      flow)

  val connected: Future[Done.type] = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      log.debug("WebSocket connected")
      Future.successful(Done)
    } else {
      throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
  }

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}


// TODO send unsubscribe message to bitfinex on shutdown event