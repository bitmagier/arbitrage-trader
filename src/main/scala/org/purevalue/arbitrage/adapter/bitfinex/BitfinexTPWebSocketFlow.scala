package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexTPWebSocketFlow.StartStreamRequest
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}


trait DecodedBitfinexMessage
case class UnknownChannelDataMessage(m: String) extends DecodedBitfinexMessage
case class JsonMessage(j: JsObject) extends DecodedBitfinexMessage
case class SubscribeRequestJson(event: String = "subscribe", channel: String, symbol: String)

case class Heartbeat() extends DecodedBitfinexMessage

case class RawOrderBookEntryJson(price: Double, count: Int, amount: Double)
object RawOrderBookEntryJson {
  def apply(v: Tuple3[Double, Int, Double]): RawOrderBookEntryJson = RawOrderBookEntryJson(v._1, v._2, v._3)
}

case class RawOrderBookSnapshotJson(channelId: Int, values: List[RawOrderBookEntryJson]) extends DecodedBitfinexMessage { // [channelId, [[price, count, amount],...]]
  def toOrderBookSnapshot: OrderBookSnapshot = {
    val bids = values
      .filter(_.count > 0)
      .filter(_.amount > 0)
      .map(e => Bid(e.price, e.amount))
    val asks = values
      .filter(_.count > 0)
      .filter(_.amount < 0)
      .map(e => Ask(e.price, -e.amount))
    OrderBookSnapshot(
      bids, asks
    )
  }
}
object RawOrderBookSnapshotJson {
  def apply(v: Tuple2[Int, List[RawOrderBookEntryJson]]): RawOrderBookSnapshotJson = RawOrderBookSnapshotJson(v._1, v._2)
}

case class RawOrderBookUpdateJson(channelId: Int, value: RawOrderBookEntryJson) extends DecodedBitfinexMessage { // [channelId, [price, count, amount]]
  private val log = LoggerFactory.getLogger(classOf[RawOrderBookUpdateJson])
  /*
    Algorithm to create and keep a book instance updated

    1. subscribe to channel
    2. receive the book snapshot and create your in-memory book structure
    3. when count > 0 then you have to add or update the price level
    3.1 if amount > 0 then add/update bids
    3.2 if amount < 0 then add/update asks
    4. when count = 0 then you have to delete the price level.
    4.1 if amount = 1 then remove from bids
    4.2 if amount = -1 then remove from asks
  */
  def toOrderBookUpdate: OrderBookUpdate = {
    if (value.count > 0) {
      if (value.amount > 0)
        OrderBookUpdate(List(Bid(value.price, value.amount)), List())
      else if (value.amount < 0)
        OrderBookUpdate(List(), List(Ask(value.price, -value.amount)))
      else {
        log.warn(s"undefined update case: $this")
        OrderBookUpdate(List(), List())
      }
    } else if (value.count == 0) {
      if (value.amount == 1.0d)
        OrderBookUpdate(List(Bid(value.price, 0.0d)), List()) // quantity == 0.0 means remove price level in our OrderBook
      else if (value.amount == -1.0d)
        OrderBookUpdate(List(), List(Ask(value.price, 0.0d))) // quantity == 0.0 means remove price level in our OrderBook
      else {
        log.warn(s"undefined update case: $this")
        OrderBookUpdate(List(), List())
      }
    } else {
      log.warn(s"undefined update case: $this")
      OrderBookUpdate(List(), List())
    }
  }
}
object RawOrderBookUpdateJson {
  def apply(v: Tuple2[Int, RawOrderBookEntryJson]): RawOrderBookUpdateJson = RawOrderBookUpdateJson(v._1, v._2)
}

case class RawTickerEntryJson(bid: Double, // Price of last highest bid
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
    Ticker(exchange, tradePair, bid, None, ask, None, Some(lastPrice))
}
object RawTickerEntryJson {
  def apply(v: Array[Double]): RawTickerEntryJson =
    RawTickerEntryJson(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9))
}

case class RawTickerJson(channelId: Int, value: RawTickerEntryJson) extends DecodedBitfinexMessage // [channelId, [bid, bidSize, ask, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low]]

object RawTickerJson {
  def apply(v: Tuple2[Int, RawTickerEntryJson]): RawTickerJson = RawTickerJson(v._1, v._2)
}


object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeRequest: RootJsonFormat[SubscribeRequestJson] = jsonFormat3(SubscribeRequestJson)

  implicit object rawOrderBookEntryFormat extends RootJsonFormat[RawOrderBookEntryJson] {
    def read(value: JsValue): RawOrderBookEntryJson = RawOrderBookEntryJson(value.convertTo[Tuple3[Double, Int, Double]])

    def write(v: RawOrderBookEntryJson): JsValue = ???
  }

  implicit object rawOrderBookSnapshotFormat extends RootJsonFormat[RawOrderBookSnapshotJson] {
    def read(value: JsValue): RawOrderBookSnapshotJson = RawOrderBookSnapshotJson(value.convertTo[Tuple2[Int, List[RawOrderBookEntryJson]]])

    def write(v: RawOrderBookSnapshotJson): JsValue = ???
  }

  implicit object rawOrderBookUpdateFormat extends RootJsonFormat[RawOrderBookUpdateJson] {
    def read(value: JsValue): RawOrderBookUpdateJson = RawOrderBookUpdateJson(value.convertTo[Tuple2[Int, RawOrderBookEntryJson]])

    def write(v: RawOrderBookUpdateJson): JsValue = ???
  }

  implicit object rawTickerFormat extends RootJsonFormat[RawTickerEntryJson] {
    def read(value: JsValue): RawTickerEntryJson = RawTickerEntryJson(value.convertTo[Array[Double]])

    def write(v: RawTickerEntryJson): JsValue = ???
  }

  implicit object rawTickerMessageFormat extends RootJsonFormat[RawTickerJson] {
    def read(value: JsValue): RawTickerJson = RawTickerJson(value.convertTo[Tuple2[Int, RawTickerEntryJson]])

    def write(v: RawTickerJson): JsValue = ???
  }
}


object BitfinexTPWebSocketFlow {
  case class StartStreamRequest(sink: Sink[DecodedBitfinexMessage, NotUsed])

  def props(config: ExchangeConfig, tradePair: BitfinexTradePair, tradePairDataStreamer: ActorRef): Props =
    Props(new BitfinexTPWebSocketFlow(config, tradePair, tradePairDataStreamer))
}

case class BitfinexTPWebSocketFlow(config: ExchangeConfig, tradePair: BitfinexTradePair, tradePairDataStreamer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexTPWebSocketFlow])

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
        case "ticker" =>
          tickerChannelId = channelId
          if (log.isTraceEnabled) log.trace(s"$tradePair: ticker channelId=$channelId")
        case "book" =>
          orderBookChannelId = channelId
          if (log.isTraceEnabled) log.trace(s"$tradePair: book channelId=$channelId")
      }
    case "error" => log.error(s"received error message: $j")
    case "info" => log.trace(s"received info message: $j")
    case _ => log.warn(s"received unidentified message: $j")
  }

  def decodeJson(s: String): DecodedBitfinexMessage = JsonMessage(JsonParser(s).asJsObject)

  def decodeDataArray(s: String): DecodedBitfinexMessage = {
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
              case TickerChannelId => JsonParser(s).convertTo[RawTickerJson]
              case OrderBookChannelId =>
                val snapshotPattern = "^\\[\\d+\\s*,\\s*\\[\\s*\\[.*".r
                snapshotPattern.findFirstIn(s) match {
                  case Some(_) => JsonParser(s).convertTo[RawOrderBookSnapshotJson]
                  case None => JsonParser(s).convertTo[RawOrderBookUpdateJson]
                }
              case id =>
                log.error(s"${Emoji.SadAndConfused}  [bitfinex:$tradePair] data message with unknown channelId $id received.")
                UnknownChannelDataMessage(s)
            }
          case None => throw new RuntimeException(s"${Emoji.SadAndConfused}  Unable to decode bifinex data message:\n$s")
        }
    }
  }

  val downStreamFlow: Flow[Message, Option[DecodedBitfinexMessage], NotUsed] = Flow.fromFunction {
    case msg: TextMessage =>
      val f: Future[Option[DecodedBitfinexMessage]] = {
        msg.toStrict(Config.httpTimeout)
          .map(_.getStrictText)
          .map {
            case s: String if s.startsWith("{") => decodeJson(s)
            case s: String if s.startsWith("[") => decodeDataArray(s)
          } map {
          case j: JsonMessage if j.j.fields.contains("event") =>
            handleEvent(j.j.fields("event").convertTo[String], j.j)
            None
          case j: JsonMessage =>
            log.warn(s"Unhandled JsonMessage received: $j")
            None
          case _: UnknownChannelDataMessage =>
            None
          case m: DecodedBitfinexMessage =>
            if (log.isTraceEnabled) log.trace(s"received: $m")
            Some(m)
          case other =>
            log.warn(s"${Emoji.Confused}  Unhandled object (for $tradePair). Message: $other")
            None
        }
      }
      try {
        Await.result(f, Config.httpTimeout.plus(1000.millis))
      } catch {
        case e: Exception => throw new RuntimeException(s"While decoding WebSocket stream event: $msg", e)
      }
    case msg: Message =>
      log.warn(s"Unexpected kind of Message received: $msg")
      None
  }

  private val DefaultSubscribeMessages = List(
    SubscribeRequestJson(channel = "ticker", symbol = tradePair.apiSymbol)
  )
  private val SubscribeMessages: List[SubscribeRequestJson] = if (config.orderBooksEnabled)
    SubscribeRequestJson(channel = "book", symbol = tradePair.apiSymbol) :: DefaultSubscribeMessages
  else DefaultSubscribeMessages

  // flow to us
  // emits a list of Messages and then keep the connection open
  def createFlowTo(sink: Sink[DecodedBitfinexMessage, NotUsed]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      downStreamFlow
        .filter(_.isDefined)
        .map(_.get)
        .toMat(sink)(Keep.right),
      Source(
        SubscribeMessages.map(m => TextMessage(m.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  val WebSocketEndpoint: Uri = Uri(s"wss://api-pub.bitfinex.com/ws/2")
  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _

  var connected: Future[Done.type] = _

  def createConnected: Future[Done.type] =
    ws._1.flatMap {
      upgrade =>
        if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
          if (log.isTraceEnabled) log.trace("WebSocket connected")
          Future.successful(Done)
        } else {
          throw new RuntimeException(s"Connection failed: ${
            upgrade.response.status
          }")
        }
    }


  override def receive: Receive = {
    case StartStreamRequest(sink) =>
      if (log.isTraceEnabled) log.trace("starting WebSocket stream")
      ws = Http().singleWebSocketRequest(
        WebSocketRequest(WebSocketEndpoint),
        createFlowTo(sink))
      connected = createConnected

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
