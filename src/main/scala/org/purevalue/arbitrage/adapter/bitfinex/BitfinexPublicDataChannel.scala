package org.purevalue.arbitrage.adapter.bitfinex

import java.time.Instant

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Kill, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.ExchangePublicDataManager.IncomingData
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataChannel.Connect
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.GetBitfinexTradePairs
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.{adapter, _}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.collection.concurrent.TrieMap
import scala.collection.{Seq, Set}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


private[bitfinex] trait IncomingPublicBitfinexJson
private[bitfinex] case class UnknownChannelDataMessage(m: String) extends IncomingPublicBitfinexJson
private[bitfinex] case class JsonMessage(j: JsObject) extends IncomingPublicBitfinexJson
private[bitfinex] case class SubscribeRequestJson(event: String = "subscribe", channel: String, symbol: String)

private[bitfinex] case class RawHeartbeat() extends IncomingPublicBitfinexJson

private[bitfinex] case class RawTickerEntryJson(bid: Double, // Price of last highest bid
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
    adapter.Ticker(exchange, tradePair, bid, None, ask, None, Some(lastPrice))
}
private[bitfinex] object RawTickerEntryJson {
  def apply(v: Vector[Double]): RawTickerEntryJson =
    RawTickerEntryJson(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9))
}

private[bitfinex] case class RawTickerJson(channelId: Int, value: RawTickerEntryJson) extends IncomingPublicBitfinexJson // [channelId, [bid, bidSize, ask, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low]]

private[bitfinex] object RawTickerJson {
  def apply(v: Tuple2[Int, RawTickerEntryJson]): RawTickerJson = RawTickerJson(v._1, v._2)
}


private[bitfinex] case class RawOrderBookEntryJson(price: Double, count: Int, amount: Double)
private[bitfinex] object RawOrderBookEntryJson {
  def apply(v: Tuple3[Double, Int, Double]): RawOrderBookEntryJson = RawOrderBookEntryJson(v._1, v._2, v._3)
}

// [channelId, [[price, count, amount],...]]
private[bitfinex] case class RawOrderBookSnapshotJson(channelId: Int, values: List[RawOrderBookEntryJson]) extends IncomingPublicBitfinexJson {
  def toOrderBook(exchange: String, tradePair: TradePair): OrderBook = {
    val bids = values
      .filter(_.count > 0)
      .filter(_.amount > 0)
      .map(e => e.price -> Bid(e.price, e.amount))
      .toMap
    val asks = values
      .filter(_.count > 0)
      .filter(_.amount < 0)
      .map(e => e.price -> Ask(e.price, -e.amount))
      .toMap
    OrderBook(exchange, tradePair, bids, asks)
  }
}
private[bitfinex] object RawOrderBookSnapshotJson {
  def apply(v: Tuple2[Int, List[RawOrderBookEntryJson]]): RawOrderBookSnapshotJson = RawOrderBookSnapshotJson(v._1, v._2)
}

private[bitfinex] case class RawOrderBookUpdateJson(channelId: Int, value: RawOrderBookEntryJson) extends IncomingPublicBitfinexJson { // [channelId, [price, count, amount]]
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
  def toOrderBookUpdate(exchange: String, tradePair: TradePair): OrderBookUpdate = {
    if (value.count > 0) {
      if (value.amount > 0)
        OrderBookUpdate(exchange, tradePair, List(Bid(value.price, value.amount)), List())
      else if (value.amount < 0)
        OrderBookUpdate(exchange, tradePair, List(), List(Ask(value.price, -value.amount)))
      else {
        log.warn(s"undefined update case: $this")
        OrderBookUpdate(exchange, tradePair, List(), List())
      }
    } else if (value.count == 0) {
      if (value.amount == 1.0d)
        OrderBookUpdate(exchange, tradePair, List(Bid(value.price, 0.0d)), List()) // quantity == 0.0 means remove price level in our OrderBook
      else if (value.amount == -1.0d)
        OrderBookUpdate(exchange, tradePair, List(), List(Ask(value.price, 0.0d))) // quantity == 0.0 means remove price level in our OrderBook
      else {
        log.warn(s"undefined update case: $this")
        OrderBookUpdate(exchange, tradePair, List(), List())
      }
    } else {
      log.warn(s"undefined update case: $this")
      OrderBookUpdate(exchange, tradePair, List(), List())
    }
  }
}
private[bitfinex] object RawOrderBookUpdateJson {
  def apply(v: Tuple2[Int, RawOrderBookEntryJson]): RawOrderBookUpdateJson = RawOrderBookUpdateJson(v._1, v._2)
}

private[bitfinex] object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeRequest: RootJsonFormat[SubscribeRequestJson] = jsonFormat3(SubscribeRequestJson)

  implicit object rawTickerFormat extends RootJsonFormat[RawTickerEntryJson] {

    def read(value: JsValue): RawTickerEntryJson = RawTickerEntryJson(value.convertTo[Vector[Double]])

    def write(v: RawTickerEntryJson): JsValue = throw new NotImplementedError
  }

  implicit object rawTickerMessageFormat extends RootJsonFormat[RawTickerJson] {
    def read(value: JsValue): RawTickerJson = RawTickerJson(value.convertTo[Tuple2[Int, RawTickerEntryJson]])

    def write(v: RawTickerJson): JsValue = throw new NotImplementedError
  }

  implicit object rawOrderBookEntryFormat extends RootJsonFormat[RawOrderBookEntryJson] {
    def read(value: JsValue): RawOrderBookEntryJson = RawOrderBookEntryJson(value.convertTo[Tuple3[Double, Int, Double]])

    def write(v: RawOrderBookEntryJson): JsValue = throw new NotImplementedError
  }

  implicit object rawOrderBookSnapshotFormat extends RootJsonFormat[RawOrderBookSnapshotJson] {
    def read(value: JsValue): RawOrderBookSnapshotJson = RawOrderBookSnapshotJson(value.convertTo[Tuple2[Int, List[RawOrderBookEntryJson]]])

    def write(v: RawOrderBookSnapshotJson): JsValue = throw new NotImplementedError
  }

  implicit object rawOrderBookUpdateFormat extends RootJsonFormat[RawOrderBookUpdateJson] {
    def read(value: JsValue): RawOrderBookUpdateJson = RawOrderBookUpdateJson(value.convertTo[Tuple2[Int, RawOrderBookEntryJson]])

    def write(v: RawOrderBookUpdateJson): JsValue = throw new NotImplementedError
  }
}


////////////////////////////////////////////////

object BitfinexPublicDataChannel {
  private case class Connect()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            exchangePublicDataManager: ActorRef,
            publicDataInquirer: ActorRef): Props =
    Props(new BitfinexPublicDataChannel(globalConfig, exchangeConfig, tradePairs, exchangePublicDataManager, publicDataInquirer))
}

/**
 * Bitfinex public data channel
 * Converts Raw data to unified ExchangeTPStreamData
 */
private[bitfinex] class BitfinexPublicDataChannel(globalConfig: GlobalConfig,
                                                  exchangeConfig: ExchangeConfig,
                                                  tradePairs: Set[TradePair],
                                                  exchangePublicDataManager: ActorRef,
                                                  publicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexPublicDataChannel])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val WebSocketEndpoint: Uri = Uri(s"wss://api-pub.bitfinex.com/ws/2")
  val MaximumNumberOfChannelsPerConnection: Int = 25

  var bitfinexTradePairByApiSymbol: Map[String, BitfinexTradePair] = _
  val tickerSymbolsByConnectionIdAndChannelId: collection.concurrent.Map[(Int, Int), String] = TrieMap() // Map[(connectionID,channelId), tickerSymbol]
  val orderBookSymbolsByConnectionIdAndChannelId: collection.concurrent.Map[(Int, Int), String] = TrieMap() // Map[(connectionID,channelId), orderBookSymbol]

  var wsList: List[(Future[WebSocketUpgradeResponse], Promise[Option[Message]])] = List()
  var connectedList: List[Future[Done.type]] = List()

  import WebSocketJsonProtocoll._

  def tickerChannelTradePair(connectionId: Int, channelId: Int): TradePair = bitfinexTradePairByApiSymbol(tickerSymbolsByConnectionIdAndChannelId((connectionId, channelId))).toTradePair

  def orderBookChannelTradepair(connectionId: Int, channelId: Int): TradePair = bitfinexTradePairByApiSymbol(orderBookSymbolsByConnectionIdAndChannelId((connectionId, channelId))).toTradePair

  def exchangeDataMapping(connectionId: Int, in: Seq[IncomingPublicBitfinexJson]): Seq[ExchangePublicStreamData] = in.map {
    // @formatter:off
    case t: RawTickerJson            => t.value.toTicker(exchangeConfig.name, tickerChannelTradePair(connectionId, t.channelId))
    case b: RawOrderBookSnapshotJson => b.toOrderBook(exchangeConfig.name, orderBookChannelTradepair(connectionId, b.channelId))
    case b: RawOrderBookUpdateJson   => b.toOrderBookUpdate(exchangeConfig.name, orderBookChannelTradepair(connectionId, b.channelId))
    case RawHeartbeat()              => Heartbeat(Instant.now)
    case other                       => log.error(s"unhandled object: $other"); throw new NotImplementedError()
    // @formatter:on
  }

  // TODO handle bitfinex Info codes:
  // 20051 : Stop/Restart Websocket Server (please reconnect)
  // 20060 : Entering in Maintenance mode. Please pause any activity and resume after receiving the info message 20061 (it should take 120 seconds at most).
  // 20061 : Maintenance ended. You can resume normal activity. It is advised to unsubscribe/subscribe again all channels.

  def handleEvent(connectionId: Int, event: String, j: JsObject): Unit = event match {
    case "subscribed" =>
      if (log.isTraceEnabled) log.trace(s"received SubscribeResponse message: $j")
      val channel = j.fields("channel").convertTo[String]
      val channelId = j.fields("chanId").convertTo[Int]

      channel match {
        case "ticker" =>
          val symbol = j.fields("symbol").convertTo[String]
          tickerSymbolsByConnectionIdAndChannelId.put((connectionId, channelId), symbol)
        case "book" =>
          val symbol = j.fields("symbol").convertTo[String]
          orderBookSymbolsByConnectionIdAndChannelId.put((connectionId, channelId), symbol)

        case _ => log.error(s"unknown channel subscribe response for: $channel")
      }
    case "error" =>
      val errorName: String = j.fields("code").convertTo[Int] match {
        case 10000 => "Unknown event"
        case 10001 => "Unknown pair"
        case 10300 => "Subscription failed (generic)"
        case 10301 => "Already subscribed"
        case 10302 => "Unknown channel"
        case 10305 => "Reached limit of open channels"
        case 10400 => "Subscription failed (generic)"
        case 10401 => "Not subscribed"
        case _ => "unknown error code"
      }
      log.error(s"received error ($errorName) message: $j")
    case "info" => log.debug(s"received info message: $j")
    case _ => log.warn(s"received unidentified message: $j")
  }

  def decodeJsonObject(s: String): IncomingPublicBitfinexJson = JsonMessage(JsonParser(s).asJsObject)

  val ChannelTypeTicker: Int = 1
  val ChannelTypeOrderBook: Int = 2

  def channelTypeWithWait(connectionId: Int, channelId: Int): Option[Int] = {
    val deadline = Instant.now.plusSeconds(1)
    do {
      if (tickerSymbolsByConnectionIdAndChannelId.contains((connectionId, channelId))) return Some(ChannelTypeTicker)
      if (orderBookSymbolsByConnectionIdAndChannelId.contains((connectionId, channelId))) return Some(ChannelTypeOrderBook)
      Thread.sleep(50) // we wait a little and try again, because in the beginning of the stream some data arrives before the channel subscribe response message is processed
    } while (Instant.now.isBefore(deadline))
    None
  }

  def decodeDataArray(connectionId: Int, dataChannelMessage: String): IncomingPublicBitfinexJson = {
    val (channelId, payload) = new BitfinexDataArrayMessageParser(dataChannelMessage).decode
    if (payload.startsWith(""""hb"""")) { // [ CHANNEL_ID, "hb" ]
      RawHeartbeat()
    } else if (payload.startsWith("[")) {
      channelTypeWithWait(connectionId, channelId) match {
        case Some(ChannelTypeTicker) => JsonParser(dataChannelMessage).convertTo[RawTickerJson] // e.g. [241965,[225.34,791.79880999,225.9,634.57980242,2.66,0.012,225.24,122.90567532,232.83,222.58]]
        case Some(ChannelTypeOrderBook) => payload match {
          case payload: String if payload.startsWith("[[") => JsonParser(dataChannelMessage).convertTo[RawOrderBookSnapshotJson] // [channelId, [[price, count, amount],...]]
          case _ => JsonParser(dataChannelMessage).convertTo[RawOrderBookUpdateJson] // [channelId, [price, count, amount]]
        }
        case Some(_) => throw new NotImplementedError()
        case None =>
          log.error(s"bitfinex: data message with unknown channelId $channelId received: $dataChannelMessage")
          UnknownChannelDataMessage(dataChannelMessage)
      }
    } else {
      log.error(s"bitfinex: Unable to decode bifinex data message:\n$dataChannelMessage")
      UnknownChannelDataMessage(dataChannelMessage)
    }
  }

  def decodeMessage(connectionId: Int, message: Message): Future[Seq[IncomingPublicBitfinexJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map {
          case s: String if s.startsWith("{") => decodeJsonObject(s)
          case s: String if s.startsWith("[") => decodeDataArray(connectionId, s)
          case x =>
            log.error(s"unidentified response: $x")
            Nil
        } map {
        case JsonMessage(j) if j.fields.contains("event") =>
          handleEvent(connectionId, j.fields("event").convertTo[String], j)
          Nil
        case j: JsonMessage =>
          log.warn(s"Unhandled JsonMessage received: $j")
          Nil
        case _: UnknownChannelDataMessage =>
          Nil
        case m: IncomingPublicBitfinexJson =>
          if (log.isTraceEnabled) log.trace(s"received: $m")
          Seq(m)
        case other =>
          log.warn(s"${Emoji.Confused}  Unhandled object $other")
          Nil
      }

    case msg: Message =>
      log.warn(s"Unexpected kind of message received: $msg")
      Future.successful(Nil)
  }


  // flow to us
  // emits a list of Messages and then keep the connection open
  def wsFlow(connectionId: Int, subscribeMessages: List[SubscribeRequestJson]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(connectionId, message)
          .map(e => exchangeDataMapping(connectionId, e))
          .map(IncomingData)
          .pipeTo(exchangePublicDataManager)
      ),
      Source(
        subscribeMessages.map(m => TextMessage(m.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  def createConnected(futureResponse: Future[WebSocketUpgradeResponse]): Future[Done.type] =
    futureResponse.flatMap {
      upgrade =>
        if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
          log.info(s"connected")
          Future.successful(Done)
        } else {
          throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
        }
    }

  private def subscribeTickerMessage(tradePair: TradePair): SubscribeRequestJson = {
    val apiSymbol = bitfinexTradePairByApiSymbol.values.find(_.toTradePair == tradePair).get.apiSymbol
    SubscribeRequestJson(channel = "ticker", symbol = apiSymbol)
  }

  private def subscribeOrderBookMessage(tradePair: TradePair): SubscribeRequestJson = {
    val apiSymbol = bitfinexTradePairByApiSymbol.values.find(_.toTradePair == tradePair).get.apiSymbol
    SubscribeRequestJson(channel = "book", symbol = apiSymbol)
  }

  def connect(): Unit = {
    log.info(s"connecting WebSockets ...")
    wsList = List()
    connectedList = List()
    var connectionId: Int = 0
    tradePairs.grouped(MaximumNumberOfChannelsPerConnection).foreach { partition =>
      log.debug(s"""starting WebSocket stream partition for Tickers ${partition.mkString(",")}""")
      val subscribeMessages: List[SubscribeRequestJson] = partition.map(e => subscribeTickerMessage(e)).toList
      connectionId += 1
      val ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow(connectionId, subscribeMessages))
      ws._2.future.onComplete(e => log.info(s"connection closed: ${e.get}"))
      wsList = ws :: wsList
      connectedList = createConnected(ws._1) :: connectedList
    }

    tradePairs.grouped(MaximumNumberOfChannelsPerConnection).foreach { partition =>
      log.debug(s"""starting WebSocket stream partition for OrderBooks ${partition.mkString(",")}""")
      val subscribeMessages: List[SubscribeRequestJson] = partition.map(e => subscribeOrderBookMessage(e)).toList
      connectionId += 1
      val ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow(connectionId, subscribeMessages))
      ws._2.future.onComplete { e =>
        log.info(s"connection closed: ${e.get}")
        self ! Kill // trigger restart
      }
      wsList = ws :: wsList
      connectedList = createConnected(ws._1) :: connectedList
    }
    log.info(s"${wsList.size} WebSockets started")
  }

  def initBitfinexTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    bitfinexTradePairByApiSymbol = Await.result(
      (publicDataInquirer ? GetBitfinexTradePairs()).mapTo[Set[BitfinexTradePair]],
      timeout.duration.plus(500.millis))
      .filter(e => tradePairs.contains(e.toTradePair))
      .map(e => (e.apiSymbol, e))
      .toMap
  }

  override def postStop(): Unit = {
    wsList
      .filterNot(_._2.isCompleted)
      .foreach { c =>
        c._2.success(None) // close open connections
    }
  }

  override def preStart() {
    try {
      log.debug(s"BitfinexPublicDataChannel initializing...")
      initBitfinexTradePairBySymbol()
      self ! Connect()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case Connect()             => connect()
    case Status.Failure(cause) => log.error("Failure received", cause)
  }
  // @formatter:on
}
