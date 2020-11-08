package org.purevalue.arbitrage.adapter.binance

import akka.Done
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.BinanceBaseRestEndpoint
import org.purevalue.arbitrage.adapter.{PublicDataChannel, PublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.traderoom.exchange.Exchange.IncomingPublicData
import org.purevalue.arbitrage.traderoom.exchange.{Exchange, ExchangePublicStreamData, Ticker, TradePairStats}
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.purevalue.arbitrage.util.{Emoji, HttpUtil}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.collection.Set
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}


private[binance] case class StreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

private[binance] trait IncomingPublicBinanceJson

private[binance] case class RawBookTickerRestJson(symbol: String,
                                                  bidPrice: String,
                                                  bidQty: String,
                                                  askPrice: String,
                                                  askQty: String) extends IncomingPublicBinanceJson {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker = {
    Ticker(exchange, resolveSymbol(symbol), bidPrice.toDouble, Some(bidQty.toDouble), askPrice.toDouble, Some(askQty.toDouble), None)
  }
}

private[binance] case class RawBookTickerStreamJson(u: Long, // order book updateId
                                                    s: String, // symbol
                                                    b: String, // best bid price
                                                    B: String, // best bid quantity
                                                    a: String, // best ask price
                                                    A: String // best ask quantity
                                                   ) extends IncomingPublicBinanceJson {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker =
    Ticker(exchange, resolveSymbol(s), b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), None)
}

private[binance] case class TickerStatisticsJson(symbol: String,
                                                 volume:String)

private[binance] object WebSocketJsonProtocol extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequestJson] = jsonFormat3(StreamSubscribeRequestJson)
  implicit val rawBookTickerRest: RootJsonFormat[RawBookTickerRestJson] = jsonFormat5(RawBookTickerRestJson)
  implicit val rawBookTickerStream: RootJsonFormat[RawBookTickerStreamJson] = jsonFormat6(RawBookTickerStreamJson)
  implicit val tickerStatisticsJson: RootJsonFormat[TickerStatisticsJson] = jsonFormat2(TickerStatisticsJson)
}


object BinancePublicDataChannel {
  def apply(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            relevantTradePairs: Set[TradePair],
            exchange: ActorRef[Exchange.Message],
            binancePublicDataInquirer: ActorRef[PublicDataInquirer.Command]):
  Behavior[PublicDataChannel.Event] = {
    Behaviors.withTimers(timers =>
      Behaviors.setup(context =>
        new BinancePublicDataChannel(context, timers, globalConfig, exchangeConfig, relevantTradePairs, exchange, binancePublicDataInquirer)))
  }
}
/**
 * Binance TradePair-based data channel
 * Converts Raw TradePair-based data to unified ExchangeTPStreamData
 */
private[binance] class BinancePublicDataChannel(context: ActorContext[PublicDataChannel.Event],
                                                timers: TimerScheduler[PublicDataChannel.Event],
                                                globalConfig: GlobalConfig,
                                                exchangeConfig: ExchangeConfig,
                                                relevantTradePairs: Set[TradePair],
                                                exchange: ActorRef[Exchange.Message],
                                                binancePublicDataInquirer: ActorRef[PublicDataInquirer.Command])
  extends PublicDataChannel(context, timers, exchangeConfig) {

  import PublicDataChannel._

  private val log = LoggerFactory.getLogger(getClass)

  val BaseRestEndpoint = "https://api.binance.com"
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/stream")
  val IdBookTickerStream: Int = 1

  @volatile var outstandingStreamSubscribeResponses: Set[Int] = Set(IdBookTickerStream)

  private var binanceTradePairBySymbol: Map[String, BinanceTradePair] = _

  import WebSocketJsonProtocol._

  val resolveTradePairSymbol: String => TradePair =
    symbol => binanceTradePairBySymbol(symbol).toTradePair

  def onStreamSubscribeResponse(j: JsObject): Unit = {
    if (log.isTraceEnabled) log.trace(s"received $j")
    val channelId = j.fields("id").convertTo[Int]
    if (outstandingStreamSubscribeResponses.nonEmpty) {
      synchronized {
        outstandingStreamSubscribeResponses = outstandingStreamSubscribeResponses - channelId
      }
      if (outstandingStreamSubscribeResponses.isEmpty) {
        context.self ! PublicDataChannel.OnStreamsRunning()
      }
    } else {
      log.warn(s"received stream subscribe response for channelId $channelId, but we were not waiting for this one ${Emoji.NoSupport}")
    }
  }

  def exchangeDataMapping(in: Seq[IncomingPublicBinanceJson]): Seq[ExchangePublicStreamData] = in.map {
    // @formatter:off
    case t: RawBookTickerRestJson   => t.toTicker(exchangeConfig.name, resolveTradePairSymbol)
    case t: RawBookTickerStreamJson => t.toTicker(exchangeConfig.name, resolveTradePairSymbol)
    case other                      =>
      log.error(s"binance unhandled object: $other")
      throw new RuntimeException()
    // @formatter:on
  }

  def decodeDataMessage(j: JsObject): Seq[IncomingPublicBinanceJson] = {
    j.fields("stream").convertTo[String] match {
      case "!bookTicker" =>
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

  def subscribeMessages: List[StreamSubscribeRequestJson] =
    List(StreamSubscribeRequestJson(params = Seq("!bookTicker"), id = IdBookTickerStream))

  // flow to us
  // emits a list of Messages and then keep the connection open
  def wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .map(exchangeDataMapping)
          .map(IncomingPublicData)
          .foreach(exchange ! _)
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
        log.info("connected")
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  def connect(): Unit = {
    log.info("initializing binance public data channel")
    initBinanceTradePairBySymbol()

    log.info(s"connecting WebSocket $WebSocketEndpoint ...")
    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    ws._2.future.onComplete { _ =>
      log.info(s"connection closed")
      context.self ! Disconnected()
    }
    connected = createConnected
  }

  def deliverBookTickerState(): Unit = {
    httpGetJson[Seq[RawBookTickerRestJson], JsValue](s"$BaseRestEndpoint/api/v3/ticker/bookTicker") onComplete {

      case Success(Left(tickers)) =>
        val rawTicker = tickers.filter(e => binanceTradePairBySymbol.keySet.contains(e.symbol))
        exchange ! IncomingPublicData(exchangeDataMapping(rawTicker))

      case Success(Right(errorResponse)) => log.error(s"deliverBookTickerState failed: $errorResponse")
      case Failure(e) => log.error("Query/Transform RawBookTickerRestJson failed", e)
    }
  }

  def initBinanceTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    binanceTradePairBySymbol = Await.result(
      binancePublicDataInquirer.ask(ref => BinancePublicDataInquirer.GetBinanceTradePairs(ref)),
      timeout.duration.plus(500.millis))
      .filter(e => relevantTradePairs.contains(e.toTradePair))
      .map(e => (e.symbol, e))
      .toMap
  }

  override def onStreamsRunning(): Unit = {
    deliverBookTickerState()
  }

  override def deliverTradePairStats(): Unit = {
    binanceTradePairBySymbol.foreach {
      case (symbol,pair) =>
        HttpUtil.httpGetJson[TickerStatisticsJson, JsValue](s"$BinanceBaseRestEndpoint/api/v3/ticker/24hr") foreach {
          case Left(tickerStats) =>
            exchange ! Exchange.IncomingPublicData(
              Seq(TradePairStats(exchangeConfig.name, pair.toTradePair, tickerStats.volume.toDouble))
            )

          case Right(error) => log.error(s"query ticker stats for ${pair.toTradePair} failed: $error")
        }
    }
  }

  override def postStop(): Unit = {
    if (ws != null && !ws._2.isCompleted) ws._2.success(None)
  }

  connect()

}

// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark