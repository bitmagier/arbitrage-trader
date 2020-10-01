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
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataChannel.{CoinbaseWebSocketEndpoint, Connect}
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataInquirer.GetCoinbaseTradePairs
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

private[coinbase] case class SubscribeRequestJson(`type`: String = "subscribe",
                                                  product_ids: Seq[String],
                                                  channels: Seq[String])

private[coinbase] trait IncomingPublicCoinbaseJson

private[coinbase] case class TickerJson(`type`: String,
//                                        trade_id: Long,
                                        sequence: Long,
                                        time: String,
                                        product_id: String,
                                        price: String,
                                        side: String,
                                        last_size: String,
                                        best_bid: String,
                                        best_ask: String
                                       ) extends IncomingPublicCoinbaseJson {
  def toTicker(exchange: String, resolveProductId: String => TradePair): Ticker = Ticker(
    exchange,
    resolveProductId(product_id),
    best_bid.toDouble,
    None,
    best_ask.toDouble,
    None,
    Some(price.toDouble))
}

private[coinbase] case class OrderBookSnapshotJson(`type`: String,
                                                   product_id: String,
                                                   bids: Vector[Tuple2[String, String]], // price,size
                                                   asks: Vector[Tuple2[String, String]] // price,size
                                                  ) extends IncomingPublicCoinbaseJson {
  def toOrderBook(exchange: String, resolveProductId: String => TradePair): OrderBook = OrderBook(
    exchange,
    resolveProductId(product_id),
    bids.map(e => Bid(e._1.toDouble, e._2.toDouble))
      .map(e => e.price -> e)
      .toMap,
    asks.map(e => Ask(e._1.toDouble, e._2.toDouble))
      .map(e => e.price -> e)
      .toMap
  )
}

private[coinbase] case class OrderBookUpdateJson(`type`: String,
                                                 product_id: String,
                                                 time: String, // 2019-08-14T20:42:27.265Z"
                                                 changes: Vector[Tuple3[String, String, String]] // side, price, side
                                                ) extends IncomingPublicCoinbaseJson {
  def toOrderBookUpdate(exchange: String, resolveProductId: String => TradePair): OrderBookUpdate = OrderBookUpdate(
    exchange,
    resolveProductId(product_id),
    changes
      .filter(_._1 == "buy")
      .map(e => Bid(e._2.toDouble, e._3.toDouble)),
    changes
      .filter(_._1 == "sell")
      .map(e => Ask(e._2.toDouble, e._3.toDouble))
  )
}

private[coinbase] object CoinbasePublicJsonProtocol extends DefaultJsonProtocol {
  implicit val subscribeRequestJson: RootJsonFormat[SubscribeRequestJson] = jsonFormat3(SubscribeRequestJson)
  implicit val tickerJson: RootJsonFormat[TickerJson] = jsonFormat9(TickerJson)
  implicit val orderBookSnapshotJson: RootJsonFormat[OrderBookSnapshotJson] = jsonFormat4(OrderBookSnapshotJson)
  implicit val orderBookUpdateJson: RootJsonFormat[OrderBookUpdateJson] = jsonFormat4(OrderBookUpdateJson)
}

object CoinbasePublicDataChannel {
  // The websocket feed is publicly available, but connections to it are rate-limited to 1 per 4 seconds per IP.
  val CoinbaseWebSocketEndpoint: String = "wss://ws-feed-public.sandbox.pro.coinbase.com" //"wss://ws-feed.pro.coinbase.com"

  private case class Connect()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            publicDataManager: ActorRef,
            coinbasePublicDataInquirer: ActorRef): Props = Props(new CoinbasePublicDataChannel(globalConfig, exchangeConfig, publicDataManager, coinbasePublicDataInquirer))
}
private[coinbase] class CoinbasePublicDataChannel(globalConfig: GlobalConfig,
                                                  exchangeConfig: ExchangeConfig,
                                                  publicDataManager: ActorRef,
                                                  coinbasePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[CoinbasePublicDataChannel])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val TickerChannelName: String = "ticker"
  val OrderBookChannelname: String = "level2"

  var coinbaseTradePairByProductId: Map[String, CoinbaseTradePair] = _

  import CoinbasePublicJsonProtocol._

  // @formatter:off
  def exchangeDataMapping(in: IncomingPublicCoinbaseJson): ExchangePublicStreamData = {
    in match {
      case t: TickerJson            => t.toTicker(exchangeConfig.name, id => coinbaseTradePairByProductId(id).toTradePair)
      case o: OrderBookSnapshotJson => o.toOrderBook(exchangeConfig.name, id => coinbaseTradePairByProductId(id).toTradePair)
      case o: OrderBookUpdateJson   => o.toOrderBookUpdate(exchangeConfig.name, id => coinbaseTradePairByProductId(id).toTradePair)
    }
  } // @formatter:on


  // @formatter:off
  def decodeJsObject(messageType: String, j: JsObject): IncomingPublicCoinbaseJson = {
    if (log.isTraceEnabled) log.trace(s"received: $j")
    messageType match {
      case TickerChannelName => j.convertTo[TickerJson]
      case "snapshot"        => j.convertTo[OrderBookSnapshotJson]
      case "l2update"        => j.convertTo[OrderBookUpdateJson]
      case "error"           => throw new RuntimeException(j.prettyPrint)
      case other             => throw new NotImplementedException(other)
    }
  } // @formatter:on

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

  def subscribeMessage: SubscribeRequestJson = SubscribeRequestJson(
    product_ids = coinbaseTradePairByProductId.keys.toSeq,
    channels = Seq(TickerChannelName, OrderBookChannelname)
  )

  // flow to us
  def wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
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
        List(TextMessage(subscribeMessage.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
  var connected: Future[Done.type] = _

  def createConnected: Future[Done.type] =
    ws._1.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  def connect(): Unit = {
    log.trace("open WebSocket stream...")

    ws = Http().singleWebSocketRequest(WebSocketRequest(CoinbaseWebSocketEndpoint), wsFlow)
    ws._2.future.onComplete { e =>
      log.info(s"connection closed: ${e.get}")
      self ! Kill
    }
    connected = createConnected
    log.info("WebSocket connected")
  }


  def initCoinbaseTradePairBySymbol(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    coinbaseTradePairByProductId = Await.result(
      (coinbasePublicDataInquirer ? GetCoinbaseTradePairs()).mapTo[Set[CoinbaseTradePair]],
      globalConfig.internalCommunicationTimeout.duration.plus(500.millis))
      .map(e => (e.id, e))
      .toMap
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
    case Status.Failure(cause) => log.error("received failure", cause)
  } // @formatter:on

}

// TODO [later] subscribe to status channel: The status channel will send all products and currencies on a preset interval.