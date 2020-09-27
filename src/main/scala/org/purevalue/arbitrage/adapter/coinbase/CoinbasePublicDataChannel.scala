package org.purevalue.arbitrage.adapter.coinbase

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Kill, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import jdk.jshell.spi.ExecutionControl.NotImplementedException
import org.purevalue.arbitrage.adapter.ExchangePublicDataManager.IncomingData
import org.purevalue.arbitrage.adapter.{ExchangePublicStreamData, Ticker}
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataChannel.{Connect, OnStreamsRunning}
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataInquirer.GetCoinbaseTradePairs
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

case class ChannelJson(name: String)
case class SubscribeRequestJson(`type`: String = "subscribe",
                                channels: Seq[ChannelJson])

trait IncomingPublicCoinbaseJson

case class TickerJson(`type`: String,
                      trade_id: Long,
                      sequence: Long,
                      time: String,
                      product_id: String,
                      price: Double,
                      side: String,
                      last_size: Double,
                      best_bid: Double,
                      best_ask: Double) extends IncomingPublicCoinbaseJson {
  def toTicker(exchange: String, resolveProductId: String => TradePair): Ticker = Ticker(
    exchange,
    resolveProductId(product_id),
    best_bid.toDouble,
    None,
    best_ask.toDouble,
    None,
    Some(price)
  )
}

object CoinbasePublicJsonProtocol extends DefaultJsonProtocol {
  implicit val channelJson: RootJsonFormat[ChannelJson] = jsonFormat1(ChannelJson)
  implicit val subscribeRequestJson: RootJsonFormat[SubscribeRequestJson] = jsonFormat2(SubscribeRequestJson)
  implicit val tickerJson: RootJsonFormat[TickerJson] = jsonFormat10(TickerJson)
}

object CoinbasePublicDataChannel {
  private case class Connect()
  private case class OnStreamsRunning()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            publicDataManager: ActorRef,
            coinbasePublicDataInquirer: ActorRef): Props = Props(new CoinbasePublicDataChannel(globalConfig, exchangeConfig, publicDataManager, coinbasePublicDataInquirer))
}
class CoinbasePublicDataChannel(globalConfig: GlobalConfig,
                                exchangeConfig: ExchangeConfig,
                                publicDataManager: ActorRef,
                                coinbasePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[CoinbasePublicDataChannel])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val BaseRestEndpoint: String = "https://api-public.sandbox.pro.coinbase.com" // https://api.pro.coinbase.com
  // The websocket feed is publicly available, but connections to it are rate-limited to 1 per 4 seconds per IP.
  val WebSocketEndpoint: String = "wss://ws-feed-public.sandbox.pro.coinbase.com" // wss://ws-feed.pro.coinbase.com

  val TickerStreamName: String = "ticker"


  var coinbaseTradePairByProductId: Map[String, CoinbaseTradePair] = _

  import CoinbasePublicJsonProtocol._

  def exchangeDataMapping(in: IncomingPublicCoinbaseJson): ExchangePublicStreamData = {
    case t: TickerJson => t.toTicker(exchangeConfig.exchangeName, id => coinbaseTradePairByProductId(id).toTradePair)
  }


  def decodeJsObject(messageType: String, j: JsObject): IncomingPublicCoinbaseJson = {
    messageType match {
      case TickerStreamName => j.convertTo[TickerJson]
      case other => throw new NotImplementedException(other)
    }
  }

  def decodeMessage(message: Message): Future[Option[IncomingPublicCoinbaseJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject() match {
          case j: JsObject if j.fields.contains("type") =>
            Some(decodeJsObject(j.fields("type").convertTo[String], j))
          case j: JsObject =>
            log.warn(s"Unknown json object received: $j")
            None
        })
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(None)
  }

  val SubscribeMessage: SubscribeRequestJson = SubscribeRequestJson(channels = Seq(
    ChannelJson(TickerStreamName),
    // TODO
  ))

  // flow to us
  val wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .filter(_.isDefined)
          .map(_.get)
          .map(exchangeDataMapping)
          .map(e => IncomingData(Seq(e)))
          .pipeTo(publicDataManager)
      ),
      Source(
        List(TextMessage(SubscribeMessage.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
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
    ws._2.future.onComplete { e =>
      log.info(s"connection closed: ${e.get}")
      self ! Kill
    }
    connected = createConnected
  }


  def initCoinbaseTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    coinbaseTradePairByProductId = Await.result(
      (coinbasePublicDataInquirer ? GetCoinbaseTradePairs()).mapTo[Set[CoinbaseTradePair]],
      globalConfig.internalCommunicationTimeout.duration.plus(500.millis))
      .map(e => (e.id, e))
      .toMap
  }

  def onStreamsRunning(): Unit = {

  }

  override def preStart() {
    try {
      log.trace(s"CoinbasePublicDataChannel initializing...")
      initCoinbaseTradePairBySymbol()
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

// TODO subscribe to status channel: The status channel will send all products and currencies on a preset interval.
