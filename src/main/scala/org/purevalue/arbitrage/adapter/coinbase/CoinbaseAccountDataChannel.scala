package org.purevalue.arbitrage.adapter.coinbase

import java.time.Instant

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Kill, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import jdk.jshell.spi.ExecutionControl.NotImplementedException
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, IncomingData, NewLimitOrder}
import org.purevalue.arbitrage.adapter.ExchangeAccountStreamData
import org.purevalue.arbitrage.adapter.coinbase.CoinbaseAccountDataChannel.{Connect, OnStreamsRunning}
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataInquirer.GetCoinbaseTradePairs
import org.purevalue.arbitrage.traderoom.{OrderStatus, OrderType, OrderUpdate, TradePair, TradeSide}
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


private[coinbase] trait IncomingCoinbaseAccountJson

private[coinbase] object CoinbaseOrder {
  def toOrderType(order_type: String): OrderType = order_type match {
    case "limit" => OrderType.LIMIT
    case "market" => OrderType.MARKET
    case "stop" => OrderType.STOP_LOSS_LIMIT
  }

  def toTradeSide(side: String): TradeSide = side match {
    case "buy" => TradeSide.Buy
    case "sell" => TradeSide.Sell
  }
}
// A valid order has been received and is now active.
// This message is emitted for every single valid order as soon as the matching engine receives it whether it fills immediately or not.
private[coinbase] case class CoinbaseOrderReceivedJson(`type`: String,
                                                       time: String, // "2014-11-07T08:19:27.028459Z",
                                                       product_id: String, //"BTC-USD",
                                                       sequence: Long, // 10,
                                                       order_id: String, // "d50ec984-77a8-460a-b958-66f114b0de9b",
                                                       size: Option[String], // "1.34", (not delivered for "market" orders)
                                                       price: Option[String], // "502.1", (not delivered for "market" orders)
                                                       side: String, // "buy",
                                                       order_type: String // "limit"
                                                      ) extends IncomingCoinbaseAccountJson {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = {
    val ts = Instant.parse(time)
    OrderUpdate(
      order_id,
      exchange,
      resolveProductId(product_id),
      CoinbaseOrder.toTradeSide(side),
      Some(CoinbaseOrder.toOrderType(order_type)),
      price.map(_.toDouble),
      None, // ?
      size.map(_.toDouble),
      Some(ts),
      Some(OrderStatus.NEW),
      None,
      None,
      ts
    )
  }
}

private[coinbase] case class CoinbaseOrderChangedJson(`type`: String,
                                                      time: String,
                                                      sequence: Long,
                                                      order_id: String,
                                                      product_id: String,
                                                      new_size: String,
                                                      old_size: String,
                                                      price: String,
                                                      side: String) extends IncomingCoinbaseAccountJson {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = OrderUpdate(
    order_id,
    exchange,
    resolveProductId(product_id),
    CoinbaseOrder.toTradeSide(side),
    None,
    Some(price.toDouble),
    None,
    Some(new_size.toDouble),
    None,
    None,
    None,
    None,
    Instant.parse(time)
  )
}

private[coinbase] case class CoinbaseOrderDoneJson(`type`: String,
                                                   time: String,
                                                   product_id: String,
                                                   sequence: Long,
                                                   price: String,
                                                   order_id: String,
                                                   reason: String, // "filled" or "canceled"
                                                   side: String,
                                                   remaining_size: String) extends IncomingCoinbaseAccountJson {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = OrderUpdate(
    order_id,
    exchange,
    resolveProductId(product_id),
    CoinbaseOrder.toTradeSide(side),
    None,
    Some(price.toDouble),
    None,
    None,
    None,
    reason match {
      case "filled" => Some(OrderStatus.FILLED)
      case "canceled" => Some(OrderStatus.CANCELED)
    },
    None,
    None,
    Instant.parse(time)
  )
}


private[coinbase] object CoinbaseAccountJsonProtocol extends DefaultJsonProtocol {
  implicit val coinbaseOrderReceivedJson: RootJsonFormat[CoinbaseOrderReceivedJson] = jsonFormat9(CoinbaseOrderReceivedJson)
  implicit val coinbaseOrderChangedJson: RootJsonFormat[CoinbaseOrderChangedJson] = jsonFormat9(CoinbaseOrderChangedJson)
  implicit val coinbaseOrderDoneJson: RootJsonFormat[CoinbaseOrderDoneJson] = jsonFormat9(CoinbaseOrderDoneJson)
}

object CoinbaseAccountDataChannel {
  private case class Connect()
  private case class OnStreamsRunning()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            exchangeAccountDataManager: ActorRef,
            exchangePublicDataInquirer: ActorRef): Props = Props(new CoinbaseAccountDataChannel(globalConfig, exchangeConfig, exchangeAccountDataManager, exchangePublicDataInquirer))
}
private[coinbase] class CoinbaseAccountDataChannel(globalConfig: GlobalConfig,
                                 exchangeConfig: ExchangeConfig,
                                 exchangeAccountDataManager: ActorRef,
                                 exchangePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[CoinbaseAccountDataChannel])

  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val UserChannelName: String = "user"

  var coinbaseTradePairsByProductId: Map[String, CoinbaseTradePair] = _
  var coinbaseTradePairsByTradePair: Map[TradePair, CoinbaseTradePair] = _

  import CoinbasePublicJsonProtocol._
  import CoinbaseAccountJsonProtocol._


  def pullCoinbaseTradePairs(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    val coinbaseTradePairs: Set[CoinbaseTradePair] = Await.result(
      (exchangePublicDataInquirer ? GetCoinbaseTradePairs()).mapTo[Set[CoinbaseTradePair]],
      timeout.duration.plus(500.millis))

    coinbaseTradePairsByProductId = coinbaseTradePairs.map(e => e.id -> e).toMap
    coinbaseTradePairsByTradePair = coinbaseTradePairs.map(e => e.toTradePair -> e).toMap
  }

  // @formatter:off
  def exchangeDataMapping(in: IncomingCoinbaseAccountJson): ExchangeAccountStreamData = in match {
    case o: CoinbaseOrderReceivedJson => o.toOrderUpdate(exchangeConfig.exchangeName, id => coinbaseTradePairsByProductId(id).toTradePair)
    case o: CoinbaseOrderChangedJson  => o.toOrderUpdate(exchangeConfig.exchangeName, id => coinbaseTradePairsByProductId(id).toTradePair)
    case o: CoinbaseOrderDoneJson     => o.toOrderUpdate(exchangeConfig.exchangeName, id => coinbaseTradePairsByProductId(id).toTradePair)
  } // @formatter:on

  // @formatter:off
  def decodeJsObject(messageType: String, j: JsObject): Option[IncomingCoinbaseAccountJson] = {
    messageType match {
      case "received"      => Some(j.convertTo[CoinbaseOrderReceivedJson])
      case "change"        => Some(j.convertTo[CoinbaseOrderChangedJson]) // An order has changed. This is the result of self-trade prevention adjusting the order size or available funds
      case "done"          => Some(j.convertTo[CoinbaseOrderDoneJson])
      case "open"          => None // ignore: This message will only be sent for orders which are not fully filled immediately.
      case "match"         => None // ignore: A trade occurred between two orders.
      case "activate"      => None // ignore: An activate message is sent when a stop order is placed.
      case other           => throw new NotImplementedException(other)
    }
  }

  def decodeMessage(message: Message): Future[Option[IncomingCoinbaseAccountJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject() match {
          case j: JsObject if j.fields.contains("type") =>
            import DefaultJsonProtocol._
            decodeJsObject(j.fields("type").convertTo[String], j)
          case j: JsObject =>
            log.warn(s"Unknown json object received: $j")
            None
        })
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(None)
  }


  val BaseRestEndpoint: String = "https://api-public.sandbox.pro.coinbase.com" // https://api.pro.coinbase.com
  // The websocket feed is publicly available, but connections to it are rate-limited to 1 per 4 seconds per IP.
  val WebSocketEndpoint: String = "wss://ws-feed-public.sandbox.pro.coinbase.com" // wss://ws-feed.pro.coinbase.com

  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
  var connected: Future[Done.type] = _

  val wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .filter(_.isDefined)
          .map(_.get)
          .map(exchangeDataMapping)
          .map(e => IncomingData(Seq(e)))
          .pipeTo(exchangeAccountDataManager)
      ),
      Source(
        List(TextMessage(SubscribeRequestJson(channels = Seq(ChannelJson(UserChannelName))).toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }


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
    log.trace(s"starting WebSocket stream using $WebSocketEndpoint")
    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    ws._2.future.onComplete { e =>
      log.info(s"connection closed: ${e.get}")
      self ! Kill
    }
    connected = createConnected
  }


  override def preStart(): Unit = {
    try {
      pullCoinbaseTradePairs()
      self ! Connect()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case Connect()                               => connect()
//    case OnStreamsRunning()                      => onStreamsRunning()
//    case CancelOrder(tradePair, externalOrderId) => cancelOrder(tradePair, externalOrderId.toLong).pipeTo(sender())
//    case NewLimitOrder(o)                        => newLimitOrder(o).pipeTo(sender())
    case Status.Failure(e)                       => log.error("failure", e)
  }
  // @formatter:on
}
