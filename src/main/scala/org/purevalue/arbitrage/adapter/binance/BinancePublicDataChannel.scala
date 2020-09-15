package org.purevalue.arbitrage.adapter.binance

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.GetBinanceTradePairs
import org.purevalue.arbitrage.traderoom.ExchangePublicDataManager.IncomingData
import org.purevalue.arbitrage.traderoom.{ExchangePublicStreamData, Ticker, TradePair}
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success}

object BinancePublicDataChannel {
  def props(config: ExchangeConfig, publicDataManager: ActorRef, binancePublicDataChannel: ActorRef): Props =
    Props(new BinancePublicDataChannel(config, publicDataManager, binancePublicDataChannel))
}
/**
 * Binance TradePair-based data channel
 * Converts Raw TradePair-based data to unified ExchangeTPStreamData
 */
class BinancePublicDataChannel(config: ExchangeConfig, publicDataManager: ActorRef, binancePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinancePublicDataChannel])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val BaseRestEndpoint = "https://api.binance.com"
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/stream")
  val IdBookTickerStreamRequest: Int = 1
  val BookTickerStreamName: String = "!bookTicker" // real-time stream

  private var binanceTradePairBySymbol: Map[String, BinanceTradePair] = _

  import WebSocketJsonProtocoll._

  val resolveTradePairSymbol: String => TradePair =
    symbol => binanceTradePairBySymbol(symbol).toTradePair

  def exchangeDataMapping(in: Seq[IncomingPublicBinanceJson]): Seq[ExchangePublicStreamData] = in.map {
    // @formatter:off
    case t: RawBookTickerRestJson   => t.toTicker(config.exchangeName, resolveTradePairSymbol)
    case t: RawBookTickerStreamJson => t.toTicker(config.exchangeName, resolveTradePairSymbol)
    case other                      =>
      log.error(s"binance unhandled object: $other")
      throw new RuntimeException()
    // @formatter:on
  }

  def decodeMessage(message: Message): Future[Seq[IncomingPublicBinanceJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(Config.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject() match {
          case j: JsObject if j.fields.contains("result") =>
            if (log.isTraceEnabled) log.trace(s"received $j")
            Nil // ignoring stream subscribe responses
          case j: JsObject if j.fields.contains("stream") =>
            j.fields("stream").convertTo[String] match {
              case BookTickerStreamName =>
                j.fields("data").convertTo[RawBookTickerStreamJson] match {
                  case t if binanceTradePairBySymbol.contains(t.s) => Seq(t)
                  case _ => Nil
                }
              case name: String =>
                log.warn(s"${Emoji.Confused}  Unhandled data stream '$name' received: $j")
                Nil
            }
          case j: JsObject =>
            log.warn(s"Unknown json object received: $j")
            Nil
        })
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(Nil)
  }

  def subscribeMessages: List[StreamSubscribeRequestJson] = List(
    StreamSubscribeRequestJson(params = Seq(BookTickerStreamName), id = IdBookTickerStreamRequest),
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

  def startWsStream(): Unit = {
    log.trace("open WebSocket stream...")

    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    connected = createConnected

    deliverBookTickerState()
  }

  // to disconnect:
  //ws._2.success(None)

  def deliverBookTickerState(): Unit = {
    httpGetJson[Seq[RawBookTickerRestJson]](s"$BaseRestEndpoint/api/v3/ticker/bookTicker") onComplete {
      case Success(tickers) =>
        val rawTicker = tickers.filter(e => binanceTradePairBySymbol.keySet.contains(e.symbol))
        publicDataManager ! IncomingData(exchangeDataMapping(rawTicker))
      case Failure(e) => log.error("Query/Transform RawBookTickerRestJson failed", e)
    }
  }

  def initBinanceTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutDuringInit
    binanceTradePairBySymbol = Await.result(
      (binancePublicDataInquirer ? GetBinanceTradePairs()).mapTo[Set[BinanceTradePair]],
      Config.internalCommunicationTimeout.duration.plus(500.millis))
      .map(e => (e.symbol, e))
      .toMap
  }

  override def preStart() {
    try {
      log.trace(s"BinancePublicDataChannel initializing...")
      initBinanceTradePairBySymbol()
      startWsStream()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

case class StreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

trait IncomingPublicBinanceJson

case class RawBookTickerRestJson(symbol: String,
                                 bidPrice: String,
                                 bidQty: String,
                                 askPrice: String,
                                 askQty: String) extends IncomingPublicBinanceJson {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker = {
    Ticker(exchange, resolveSymbol(symbol), bidPrice.toDouble, Some(bidQty.toDouble), askPrice.toDouble, Some(askQty.toDouble), None)
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
    Ticker(exchange, resolveSymbol(s), b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), None)
}

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequestJson] = jsonFormat3(StreamSubscribeRequestJson)
  implicit val rawBookTickerRest: RootJsonFormat[RawBookTickerRestJson] = jsonFormat5(RawBookTickerRestJson)
  implicit val rawBookTickerStream: RootJsonFormat[RawBookTickerStreamJson] = jsonFormat6(RawBookTickerStreamJson)
}
