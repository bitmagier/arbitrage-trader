package org.purevalue.arbitrage.adapter.binance

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Kill, PoisonPill, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.ExchangePublicDataManager.IncomingData
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataChannel.{Connect, OnStreamsRunning}
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.GetBinanceTradePairs
import org.purevalue.arbitrage.adapter.{ExchangePublicStreamData, Ticker}
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.purevalue.arbitrage.{adapter, _}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success}

object BinancePublicDataChannel {
  private case class Connect()
  private case class OnStreamsRunning()

  def props(globalConfig: GlobalConfig, exchangeConfig: ExchangeConfig, publicDataManager: ActorRef, binancePublicDataChannel: ActorRef): Props =
    Props(new BinancePublicDataChannel(globalConfig, exchangeConfig, publicDataManager, binancePublicDataChannel))
}
/**
 * Binance TradePair-based data channel
 * Converts Raw TradePair-based data to unified ExchangeTPStreamData
 */
class BinancePublicDataChannel(globalConfig: GlobalConfig,
                               exchangeConfig: ExchangeConfig,
                               publicDataManager: ActorRef,
                               binancePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinancePublicDataChannel])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val BaseRestEndpoint = "https://api.binance.com"
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/stream")
  val IdBookTickerStream: Int = 1
  val BookTickerStreamName: String = "!bookTicker" // real-time stream

  @volatile var outstandingStreamSubscribeResponses: Set[Int] = Set(IdBookTickerStream)

  private var binanceTradePairBySymbol: Map[String, BinanceTradePair] = _

  import WebSocketJsonProtocol._

  val resolveTradePairSymbol: String => TradePair =
    symbol => binanceTradePairBySymbol(symbol).toTradePair

  def onStreamSubscribeResponse(j: JsObject): Unit = {
    if (log.isTraceEnabled) log.trace(s"received $j")
    val channelId = j.fields("id").convertTo[Int]
    synchronized {
      outstandingStreamSubscribeResponses = outstandingStreamSubscribeResponses - channelId
    }
    if (outstandingStreamSubscribeResponses.isEmpty) {
      self ! OnStreamsRunning()
    }
  }

  def exchangeDataMapping(in: Seq[IncomingPublicBinanceJson]): Seq[ExchangePublicStreamData] = in.map {
    // @formatter:off
    case t: RawBookTickerRestJson   => t.toTicker(exchangeConfig.exchangeName, resolveTradePairSymbol)
    case t: RawBookTickerStreamJson => t.toTicker(exchangeConfig.exchangeName, resolveTradePairSymbol)
    case other                      =>
      log.error(s"binance unhandled object: $other")
      throw new RuntimeException()
    // @formatter:on
  }

  def decodeDataMessage(j: JsObject): Seq[IncomingPublicBinanceJson] = {
    j.fields("stream").convertTo[String] match {
      case BookTickerStreamName =>
        j.fields("data").convertTo[RawBookTickerStreamJson] match {
          case t if binanceTradePairBySymbol.contains(t.s) => Seq(t)
          case other =>
            if (log.isTraceEnabled) log.trace(s"ignoring data message, because its not in our symbol list: $other")
            Nil
        }
      case name: String =>
        log.warn(s"${Emoji.Confused}  Unhandled data stream '$name' received: $j")
        Nil
    }
  }

  def decodeMessage(message: Message): Future[Seq[IncomingPublicBinanceJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject() match {
          case j: JsObject if j.fields.contains("result") =>
            onStreamSubscribeResponse(j)
            Nil
          case j: JsObject if j.fields.contains("stream") =>
            decodeDataMessage(j)
          case j: JsObject =>
            log.warn(s"Unknown json object received: $j")
            Nil
        })
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(Nil)
  }

  def subscribeMessages: List[StreamSubscribeRequestJson] = List(
    StreamSubscribeRequestJson(params = Seq(BookTickerStreamName), id = IdBookTickerStream),
  )

  // flow to us
  // emits a list of Messages and then keep the connection open
  val wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .map(exchangeDataMapping)
          .map(IncomingData)
          .pipeTo(publicDataManager)
      ),
      Source(
        subscribeMessages.map(msg => TextMessage(msg.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }


  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
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

  def connect(): Unit = {
    log.trace("open WebSocket stream...")

    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    ws._2.future.onComplete{e =>
      log.info(s"connection closed: ${e.get}")
      self ! Kill // trigger restart
    }
    connected = createConnected
  }

  // to disconnect:
  //ws._2.success(None)

  def deliverBookTickerState(): Unit = {
    httpGetJson[Seq[RawBookTickerRestJson], JsValue](s"$BaseRestEndpoint/api/v3/ticker/bookTicker") onComplete {
      case Success(Left(tickers)) =>
        val rawTicker = tickers.filter(e => binanceTradePairBySymbol.keySet.contains(e.symbol))
        publicDataManager ! IncomingData(exchangeDataMapping(rawTicker))
      case Success(Right(errorResponse)) => log.error(s"deliverBookTickerState failed: $errorResponse")
      case Failure(e) => log.error("Query/Transform RawBookTickerRestJson failed", e)
    }
  }

  def initBinanceTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    binanceTradePairBySymbol = Await.result(
      (binancePublicDataInquirer ? GetBinanceTradePairs()).mapTo[Set[BinanceTradePair]],
      globalConfig.internalCommunicationTimeout.duration.plus(500.millis))
      .map(e => (e.symbol, e))
      .toMap
  }

  def onStreamsRunning(): Unit = {
    deliverBookTickerState()
  }

  override def preStart() {
    try {
      log.trace(s"BinancePublicDataChannel initializing...")
      initBinanceTradePairBySymbol()
      self ! Connect()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case Connect()             => connect()
    case OnStreamsRunning()    => onStreamsRunning()
    case Status.Failure(cause) => log.error("received failure", cause)
  }
  // @formatter:on
}

case class StreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

trait IncomingPublicBinanceJson

case class RawBookTickerRestJson(symbol: String,
                                 bidPrice: String,
                                 bidQty: String,
                                 askPrice: String,
                                 askQty: String) extends IncomingPublicBinanceJson {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker = {
    adapter.Ticker(exchange, resolveSymbol(symbol), bidPrice.toDouble, Some(bidQty.toDouble), askPrice.toDouble, Some(askQty.toDouble), None)
  }
}

case class RawBookTickerStreamJson(u: Long, // order book updateId
                                   s: String, // symbol
                                   b: String, // best bid price
                                   B: String, // best bid quantity
                                   a: String, // best ask price
                                   A: String // best ask quantity
                                  ) extends IncomingPublicBinanceJson {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker =
    adapter.Ticker(exchange, resolveSymbol(s), b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), None)
}

object WebSocketJsonProtocol extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequestJson] = jsonFormat3(StreamSubscribeRequestJson)
  implicit val rawBookTickerRest: RootJsonFormat[RawBookTickerRestJson] = jsonFormat5(RawBookTickerRestJson)
  implicit val rawBookTickerStream: RootJsonFormat[RawBookTickerStreamJson] = jsonFormat6(RawBookTickerStreamJson)
}

// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark