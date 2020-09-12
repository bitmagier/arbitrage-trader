package org.purevalue.arbitrage.adapter.bitfinex

import java.time.Instant

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.GetBitfinexTradePairs
import org.purevalue.arbitrage.traderoom.ExchangePublicDataManager.StartStreamRequest
import org.purevalue.arbitrage.traderoom.{ExchangePublicStreamData, Heartbeat, Ticker, TradePair}
import org.purevalue.arbitrage.util.Emoji
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


trait IncomingPublicBitfinexJson
case class UnknownChannelDataMessage(m: String) extends IncomingPublicBitfinexJson
case class JsonMessage(j: JsObject) extends IncomingPublicBitfinexJson
case class SubscribeRequestJson(event: String = "subscribe", channel: String, symbol: String)

case class RawHeartbeat() extends IncomingPublicBitfinexJson

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

case class RawTickerJson(channelId: Int, value: RawTickerEntryJson) extends IncomingPublicBitfinexJson // [channelId, [bid, bidSize, ask, askSize, dailyChange, dailyChangeRelative, lastPrice, volume, high, low]]

object RawTickerJson {
  def apply(v: Tuple2[Int, RawTickerEntryJson]): RawTickerJson = RawTickerJson(v._1, v._2)
}


object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeRequest: RootJsonFormat[SubscribeRequestJson] = jsonFormat3(SubscribeRequestJson)


  implicit object rawTickerFormat extends RootJsonFormat[RawTickerEntryJson] {
    def read(value: JsValue): RawTickerEntryJson = RawTickerEntryJson(value.convertTo[Array[Double]])

    def write(v: RawTickerEntryJson): JsValue = throw new NotImplementedError
  }

  implicit object rawTickerMessageFormat extends RootJsonFormat[RawTickerJson] {
    def read(value: JsValue): RawTickerJson = RawTickerJson(value.convertTo[Tuple2[Int, RawTickerEntryJson]])

    def write(v: RawTickerJson): JsValue = throw new NotImplementedError
  }


  //  implicit object rawOrderBookEntryFormat extends RootJsonFormat[RawOrderBookEntryJson] {
  //    def read(value: JsValue): RawOrderBookEntryJson = RawOrderBookEntryJson(value.convertTo[Tuple3[Double, Int, Double]])
  //
  //    def write(v: RawOrderBookEntryJson): JsValue = throw new NotImplementedError
  //  }
  //
  //  implicit object rawOrderBookSnapshotFormat extends RootJsonFormat[RawOrderBookSnapshotJson] {
  //    def read(value: JsValue): RawOrderBookSnapshotJson = RawOrderBookSnapshotJson(value.convertTo[Tuple2[Int, List[RawOrderBookEntryJson]]])
  //
  //    def write(v: RawOrderBookSnapshotJson): JsValue = throw new NotImplementedError
  //  }
  //
  //  implicit object rawOrderBookUpdateFormat extends RootJsonFormat[RawOrderBookUpdateJson] {
  //    def read(value: JsValue): RawOrderBookUpdateJson = RawOrderBookUpdateJson(value.convertTo[Tuple2[Int, RawOrderBookEntryJson]])
  //
  //    def write(v: RawOrderBookUpdateJson): JsValue = throw new NotImplementedError
  //  }
}


object BitfinexPublicDataChannel {
  def props(config: ExchangeConfig, bitfinexPublicDataInquirer: ActorRef): Props =
    Props(new BitfinexPublicDataChannel(config, bitfinexPublicDataInquirer))
}

/**
 * Bitfinex public data channel
 * Converts Raw data to unified ExchangeTPStreamData
 */
class BitfinexPublicDataChannel(config: ExchangeConfig, bitfinexPublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexPublicDataChannel])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val WebSocketEndpoint: Uri = Uri(s"wss://api-pub.bitfinex.com/ws/2")
  val MaximumNumberOfChannelsPerConnection: Int = 25

  var bitfinexTradePairBySymbol: Map[String, BitfinexTradePair] = _
  var wsList: List[(Future[WebSocketUpgradeResponse], Promise[Option[Message]])] = List()
  var connectedList: List[Future[Done.type]] = List()

  import WebSocketJsonProtocoll._

  //
  //  def createFanInSinksTo(numSinks:Int, downstreamSink: Sink[Seq[ExchangePublicStreamData], Future[Done]]): Seq[Sink[IncomingPublicBitfinexJson, NotUsed]] = {
  //    class MsgFanInShape[In, Out](_init: Init[Out] = Name("BitfinexStreamCombiner")) extends FanInShape[Out](_init) {
  //      protected override def construct(i: Init[Out]) = new MsgFanInShape(i)
  //
  //      val sink1: Inlet[In] = newInlet[In]("name1")
  //      val sink2: Inlet[In] = newInlet[In]("name2")
  //      // Outlet[Out] with name "out" is automatically created
  //    }
  //
  //    new MsgFanInShape[IncomingBitfinexAccountJson, IncomingBitfinexAccountJson]().
  //
  //  }

  // handles one partition of incoming data
  class FlowPart() {
    var tickerSymbolsByChannelId: Map[Int, String] = Map()

    def tickerChannelIdToTradePair(channelId: Int): TradePair = bitfinexTradePairBySymbol(tickerSymbolsByChannelId(channelId))

    def streamMapping(in: IncomingPublicBitfinexJson): Seq[ExchangePublicStreamData] = in match {
      // @formatter:off
      case t: RawTickerJson => Seq(t.value.toTicker(config.exchangeName, tickerChannelIdToTradePair(t.channelId)))
      case RawHeartbeat()   => Seq(Heartbeat(Instant.now))
      case other            => throw new NotImplementedError(s"unhandled: $other")
      // @formatter:on
      //    case o: RawOrderBookSnapshotJson =>
      //      Seq(o.toOrderBookSnapshot)
      //
      //    case o: RawOrderBookUpdateJson =>
      //      Seq(o.toOrderBookUpdate)
    }

    def createSinkTo(downstreamSink: Sink[Seq[ExchangePublicStreamData], Future[Done]]): Sink[IncomingPublicBitfinexJson, NotUsed] = {
      Flow.fromFunction(streamMapping).toMat(downstreamSink)(Keep.none)
    }

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
            val symbol = j.fields("symbol").convertTo[String]
            if (symbol.startsWith("t")) {
              val symbolWithoutLeadingT = symbol.substring(1)
              tickerSymbolsByChannelId = tickerSymbolsByChannelId + (channelId -> symbolWithoutLeadingT)
            } else throw new RuntimeException("basic assumption proved wrong")
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
      case "info" => log.trace(s"received info message: $j")
      case _ => log.warn(s"received unidentified message: $j")
    }

    def decodeJsonObject(s: String): IncomingPublicBitfinexJson = JsonMessage(JsonParser(s).asJsObject)

    def decodeDataArray(s: String): IncomingPublicBitfinexJson = {
      val heatBeatPattern = """^\[\s*\d+,\s*"hb"\s*]""".r
      heatBeatPattern.findFirstIn(s) match {
        case Some(_) => RawHeartbeat()
        case None =>
          val channelIdDecodePatter = """^\[(\d+)\s*,.*""".r
          channelIdDecodePatter.findFirstMatchIn(s) match {
            case Some(m) =>
              m.group(1).toInt match {
                case tickerChannelId if tickerSymbolsByChannelId.keySet.contains(tickerChannelId) =>
                  JsonParser(s).convertTo[RawTickerJson]
                //              case OrderBookChannelId =>
                //                val snapshotPattern = "^\\[\\d+\\s*,\\s*\\[\\s*\\[.*".r
                //                snapshotPattern.findFirstIn(s) match {
                //                  case Some(_) => JsonParser(s).convertTo[RawOrderBookSnapshotJson]
                //                  case None => JsonParser(s).convertTo[RawOrderBookUpdateJson]
                //                }
                case id =>
                  log.error(s"[bitfinex] data message with unknown channelId $id received.")
                  UnknownChannelDataMessage(s)
              }
            case None => throw new RuntimeException(s"Unable to decode bifinex data message:\n$s")
          }
      }
    }

    val wsFlow: Flow[Message, Option[IncomingPublicBitfinexJson], NotUsed] = Flow.fromFunction {
      case msg: TextMessage =>
        val f: Future[Option[IncomingPublicBitfinexJson]] = {
          msg.toStrict(Config.httpTimeout)
            .map(_.getStrictText)
            .map {
              case s: String if s.startsWith("{") => decodeJsonObject(s)
              case s: String if s.startsWith("[") => decodeDataArray(s)
              case x => throw new RuntimeException(s"unidentified response: $x")
            } map {
            case j: JsonMessage if j.j.fields.contains("event") =>
              handleEvent(j.j.fields("event").convertTo[String], j.j)
              None
            case j: JsonMessage =>
              log.warn(s"Unhandled JsonMessage received: $j")
              None
            case _: UnknownChannelDataMessage =>
              None
            case m: IncomingPublicBitfinexJson =>
              if (log.isTraceEnabled) log.trace(s"received: $m")
              Some(m)
            case other =>
              log.warn(s"${Emoji.Confused}  Unhandled object $other")
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


  }


  // flow to us
  // emits a list of Messages and then keep the connection open
  def createWSFlowTo(wsFlow: Flow[Message, Option[IncomingPublicBitfinexJson], NotUsed],
                     sink: Sink[IncomingPublicBitfinexJson, NotUsed],
                     subscribeMessages: List[SubscribeRequestJson]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      wsFlow
        .filter(_.isDefined)
        .map(_.get)
        .toMat(sink)(Keep.right),
      Source(
        subscribeMessages.map(m => TextMessage(m.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  def createConnected(futureResponse: Future[WebSocketUpgradeResponse]): Future[Done.type] =
    futureResponse.flatMap {
      upgrade =>
        if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
          if (log.isTraceEnabled) log.trace("WebSocket connected")
          Future.successful(Done)
        } else {
          throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
        }
    }

  private def subscribeMessage(tradePair:BitfinexTradePair): SubscribeRequestJson =
    SubscribeRequestJson(channel = "ticker", symbol = tradePair.apiSymbol)

  override def preStart() {
    if (log.isTraceEnabled()) log.trace(s"BitfinexPublicDataChannel initializing...")
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutDuringInit
    bitfinexTradePairBySymbol = Await.result(
      (bitfinexPublicDataInquirer ? GetBitfinexTradePairs()).mapTo[Set[BitfinexTradePair]],
      timeout.duration.plus(500.millis))
      .map(e => (e.apiSymbol, e))
      .toMap
  }


  override def receive: Receive = {
    case StartStreamRequest(downstreamSink) =>
      bitfinexTradePairBySymbol.values.sliding(MaximumNumberOfChannelsPerConnection).foreach { group =>
        if (log.isTraceEnabled) log.trace("starting a WebSocket stream partition")
        val subscribeMessages: List[SubscribeRequestJson] = group.map(e => subscribeMessage(e)).toList
        val flowPart = new FlowPart()
        val sink: Sink[IncomingPublicBitfinexJson, NotUsed] = flowPart.createSinkTo(downstreamSink)
        val ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), createWSFlowTo(flowPart.wsFlow, sink, subscribeMessages))
        wsList = ws :: wsList
        connectedList = createConnected(ws._1) :: connectedList
      }

    case Status.Failure(cause) =>
      log.error("Failure received", cause)
  }
}

//case class RawOrderBookEntryJson(price: Double, count: Int, amount: Double)
//object RawOrderBookEntryJson {
//  def apply(v: Tuple3[Double, Int, Double]): RawOrderBookEntryJson = RawOrderBookEntryJson(v._1, v._2, v._3)
//}
//
//case class RawOrderBookSnapshotJson(channelId: Int, values: List[RawOrderBookEntryJson]) extends IncomingPublicBitfinexJson { // [channelId, [[price, count, amount],...]]
//  def toOrderBookSnapshot: OrderBookSnapshot = {
//    val bids = values
//      .filter(_.count > 0)
//      .filter(_.amount > 0)
//      .map(e => Bid(e.price, e.amount))
//    val asks = values
//      .filter(_.count > 0)
//      .filter(_.amount < 0)
//      .map(e => Ask(e.price, -e.amount))
//    traderoom.OrderBookSnapshot(
//      bids, asks
//    )
//  }
//}
//object RawOrderBookSnapshotJson {
//  def apply(v: Tuple2[Int, List[RawOrderBookEntryJson]]): RawOrderBookSnapshotJson = RawOrderBookSnapshotJson(v._1, v._2)
//}
//
//case class RawOrderBookUpdateJson(channelId: Int, value: RawOrderBookEntryJson) extends IncomingPublicBitfinexJson { // [channelId, [price, count, amount]]
//  private val log = LoggerFactory.getLogger(classOf[RawOrderBookUpdateJson])
//
//  /*
//    Algorithm to create and keep a book instance updated
//
//    1. subscribe to channel
//    2. receive the book snapshot and create your in-memory book structure
//    3. when count > 0 then you have to add or update the price level
//    3.1 if amount > 0 then add/update bids
//    3.2 if amount < 0 then add/update asks
//    4. when count = 0 then you have to delete the price level.
//    4.1 if amount = 1 then remove from bids
//    4.2 if amount = -1 then remove from asks
//  */
//  def toOrderBookUpdate: OrderBookUpdate = {
//    if (value.count > 0) {
//      if (value.amount > 0)
//        OrderBookUpdate(List(Bid(value.price, value.amount)), List())
//      else if (value.amount < 0)
//        OrderBookUpdate(List(), List(Ask(value.price, -value.amount)))
//      else {
//        log.warn(s"undefined update case: $this")
//        OrderBookUpdate(List(), List())
//      }
//    } else if (value.count == 0) {
//      if (value.amount == 1.0d)
//        OrderBookUpdate(List(Bid(value.price, 0.0d)), List()) // quantity == 0.0 means remove price level in our OrderBook
//      else if (value.amount == -1.0d)
//        OrderBookUpdate(List(), List(Ask(value.price, 0.0d))) // quantity == 0.0 means remove price level in our OrderBook
//      else {
//        log.warn(s"undefined update case: $this")
//        OrderBookUpdate(List(), List())
//      }
//    } else {
//      log.warn(s"undefined update case: $this")
//      OrderBookUpdate(List(), List())
//    }
//  }
//}
//object RawOrderBookUpdateJson {
//  def apply(v: Tuple2[Int, RawOrderBookEntryJson]): RawOrderBookUpdateJson = RawOrderBookUpdateJson(v._1, v._2)
//}
