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
import org.purevalue.arbitrage.adapter.{PublicDataChannel, PublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.traderoom.exchange.Exchange.IncomingPublicData
import org.purevalue.arbitrage.traderoom.exchange.{Ask, Bid, Exchange, ExchangePublicStreamData, OrderBook, OrderBookUpdate, Ticker}
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.collection.Set
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}


private[binance] case class StreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

private[binance] trait IncomingPublicBinanceJson

private[binance] case class BookTickerRestJson(symbol: String,
                                               bidPrice: String,
                                               bidQty: String,
                                               askPrice: String,
                                               askQty: String) {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker = {
    Ticker(exchange, resolveSymbol(symbol), bidPrice.toDouble, Some(bidQty.toDouble), askPrice.toDouble, Some(askQty.toDouble), None)
  }
}

private[binance] case class OrderBookRestJson(lastUpdateId: Long,
                                              bids: Vector[Tuple2[String, String]],
                                              asks: Vector[Tuple2[String, String]]) extends IncomingPublicBinanceJson {
  def toOrderBook(exchange: String, pair: TradePair): OrderBook = {
    OrderBook(
      exchange,
      pair,
      bids.map(e => e._1.toDouble -> Bid(e._1.toDouble, e._2.toDouble)).toMap,
      asks.map(e => e._1.toDouble -> Ask(e._1.toDouble, e._2.toDouble)).toMap
    )
  }
}

private[binance] case class BookTickerStreamJson(u: Long, // order book updateId
                                                 s: String, // symbol
                                                 b: String, // best bid price
                                                 B: String, // best bid quantity
                                                 a: String, // best ask price
                                                 A: String // best ask quantity
                                                ) extends IncomingPublicBinanceJson {
  def toTicker(exchange: String, resolveSymbol: String => TradePair): Ticker =
    Ticker(exchange, resolveSymbol(s), b.toDouble, Some(B.toDouble), a.toDouble, Some(A.toDouble), None)
}

private[binance] case class DiffDepthStreamJson(e: String,
                                                E: Long,
                                                s: String,
                                                U: Long,
                                                u: Long,
                                                b: Vector[Tuple2[String, String]],
                                                a: Vector[Tuple2[String, String]]) extends IncomingPublicBinanceJson {
  def toOrderBookUpdate(exchange: String, resolveSymbol: String => TradePair): OrderBookUpdate =
    OrderBookUpdate(
      exchange,
      resolveSymbol(s),
      b.map(e => Bid(e._1.toDouble, e._2.toDouble)),
      a.map(e => Ask(e._1.toDouble, e._2.toDouble))
    )
}

private[binance] object WebSocketJsonProtocol extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequestJson] = jsonFormat3(StreamSubscribeRequestJson)
  implicit val bookTickerRestJson: RootJsonFormat[BookTickerRestJson] = jsonFormat5(BookTickerRestJson)
  implicit val bookTickerStreamJson: RootJsonFormat[BookTickerStreamJson] = jsonFormat6(BookTickerStreamJson)
  implicit val orderBookRestJson: RootJsonFormat[OrderBookRestJson] = jsonFormat3(OrderBookRestJson)
  implicit val diffDepthStreamJson: RootJsonFormat[DiffDepthStreamJson] = jsonFormat7(DiffDepthStreamJson)
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

  val BinanceBaseRestEndpoint = "https://api.binance.com"
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/stream")
  val IdBookTickerStream: Int = 1
  val IdDiffDepthStream: Int = 2

  @volatile var outstandingStreamSubscribeResponses: Set[Int] = Set(IdBookTickerStream, IdDiffDepthStream)

  private var binanceTradePairBySymbol: Map[String, BinanceTradePair] = _

  import WebSocketJsonProtocol._

  def resolveTradePairSymbol(symbol: String): TradePair = binanceTradePairBySymbol(symbol).toTradePair

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
    case t: BookTickerStreamJson => t.toTicker(exchangeConfig.name, resolveTradePairSymbol)
    case b: DiffDepthStreamJson  => b.toOrderBookUpdate(exchangeConfig.name, resolveTradePairSymbol)
    case other                   =>
      log.error(s"binance unhandled object: $other")
      throw new RuntimeException()
    // @formatter:on
  }

  def decodeDataMessage(j: JsObject): Seq[IncomingPublicBinanceJson] = {
    j.fields("stream").convertTo[String] match {
      case "!bookTicker" =>
        j.fields("data").convertTo[BookTickerStreamJson] match {
          case t if binanceTradePairBySymbol.contains(t.s) => Seq(t)
          case other =>
            if (log.isTraceEnabled) log.trace(s"ignoring data message, because its not in our symbol list: $other")
            Nil
        }

      case s: String if s.contains("@depth") =>
        Seq(j.fields("data").convertTo[DiffDepthStreamJson])

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
    StreamSubscribeRequestJson(params = Seq("!bookTicker"), id = IdBookTickerStream),
    StreamSubscribeRequestJson(params = relevantTradePairs.map(e =>
      s"${binanceTradePairBySymbol.values.find(_.toTradePair == e).get.symbol}@depth").toSeq, id = IdDiffDepthStream) // TODO test depth@100ms
  )

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

  def initBinanceTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    binanceTradePairBySymbol = Await.result(
      binancePublicDataInquirer.ask(ref => BinancePublicDataInquirer.GetBinanceTradePairs(ref)),
      timeout.duration.plus(500.millis))
      .filter(e => relevantTradePairs.contains(e.toTradePair))
      .map(e => (e.symbol, e))
      .toMap
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
    httpGetJson[Seq[BookTickerRestJson], JsValue](s"$BinanceBaseRestEndpoint/api/v3/ticker/bookTicker") onComplete {
      case Success(Left(tickers)) =>
        val rawTicker: Seq[BookTickerRestJson] = tickers.filter(e => binanceTradePairBySymbol.keySet.contains(e.symbol))
        exchange ! IncomingPublicData(
          rawTicker.map(_.toTicker(exchangeConfig.name, resolveTradePairSymbol))
        )
      case Success(Right(errorResponse)) => log.error(s"deliverBookTickerState failed: $errorResponse")
      case Failure(e) => log.error("Query/Transform BookTickerRestJson failed", e)
    }
  }

  def deliverOrderBooks(): Unit = {
    relevantTradePairs.foreach { pair =>
      val symbol = binanceTradePairBySymbol.values.find(_.toTradePair == pair).get.symbol
        httpGetJson[OrderBookRestJson, JsValue](s"$BinanceBaseRestEndpoint/api/v3/depth?symbol=$symbol&limit=1000") onComplete {
        case Success(Left(book)) => // TODO the pool currently does not process requests fast enough to handle the incoming request load
          exchange ! IncomingPublicData(
            Seq(book.toOrderBook(exchangeConfig.name, pair))
          )
        case Success(Right(errorResponse)) => log.error(s"deliverOrderBooks failed: $errorResponse")
        case Failure(e) => log.error("Query/Transform OrderBookRestJson failed", e)
      }
    }
  }

  override def onStreamsRunning(): Unit = {
    deliverBookTickerState()
//    deliverOrderBooks()
  }

  override def postStop(): Unit = {
    if (ws != null && !ws._2.isCompleted) ws._2.success(None)
  }

  connect()

}

// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark