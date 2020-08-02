package org.purevalue.arbitrage.adapter.bitfinex

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.scaladsl._
import org.purevalue.arbitrage.{ExchangeConfig, Main}
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

trait DecodedMessage
case class JsonMessage(j:JsObject) extends DecodedMessage
case class SubscribeRequest(event: String = "subscribe", channel: String = "book", symbol: String, prec: String = "P0", freq: String = "F0")

case class RawOrderBookEntry(price:Double, count:Int, amount:Double)
object RawOrderBookEntry {
  def apply(v:Tuple3[Double,Int,Double]):RawOrderBookEntry = RawOrderBookEntry(v._1, v._2, v._3)
}
case class RawOrderBookSnapshot(channelId:Int, values:List[RawOrderBookEntry]) extends DecodedMessage // [channelId, [[price, count, amount],...]]
object RawOrderBookSnapshot {
  def apply(v:Tuple2[Int, List[RawOrderBookEntry]]): RawOrderBookSnapshot = RawOrderBookSnapshot(v._1, v._2)
}
case class RawOrderBookUpdate(channelId:Int, value: RawOrderBookEntry) extends DecodedMessage // [channelId, [price, count, amount]]
object RawOrderBookUpdate {
  def apply(v:Tuple2[Int, RawOrderBookEntry]): RawOrderBookUpdate = RawOrderBookUpdate(v._1, v._2)
}

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeRequest: RootJsonFormat[SubscribeRequest] = jsonFormat5(SubscribeRequest)

  implicit object rawOrderBookEntryFormat extends RootJsonFormat[RawOrderBookEntry] {
    def read(value:JsValue): RawOrderBookEntry = RawOrderBookEntry(value.convertTo[Tuple3[Double,Int,Double]])
    def write(v:RawOrderBookEntry): JsValue = null
  }

  implicit object rawOrderBookSnapshotFormat extends RootJsonFormat[RawOrderBookSnapshot] {
    def read(value: JsValue): RawOrderBookSnapshot = RawOrderBookSnapshot(value.convertTo[Tuple2[Int, List[RawOrderBookEntry]]])
    def write(v: RawOrderBookSnapshot): JsValue = null
  }

  implicit object rawOrderBookUpdate extends RootJsonFormat[RawOrderBookUpdate] {
    def read(value:JsValue): RawOrderBookUpdate = RawOrderBookUpdate(value.convertTo[Tuple2[Int, RawOrderBookEntry]])
    def write(v: RawOrderBookUpdate): JsValue = null
  }
}


object BitfinexOrderBookWebSocketFlow {
  def props(config: ExchangeConfig, tradePair: BitfinexTradePair, receiver: ActorRef): Props =
    Props(new BitfinexOrderBookWebSocketFlow(config, tradePair, receiver))
}

case class BitfinexOrderBookWebSocketFlow(config: ExchangeConfig, tradePair: BitfinexTradePair, receiver: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexOrderBookWebSocketFlow])
  implicit val actorSystem: ActorSystem = Main.actorSystem

  import actorSystem.dispatcher
  import WebSocketJsonProtocoll._

  // TODO handle bitfinex Info codes:
  // 20051 : Stop/Restart Websocket Server (please reconnect)
  // 20060 : Entering in Maintenance mode. Please pause any activity and resume after receiving the info message 20061 (it should take 120 seconds at most).
  // 20061 : Maintenance ended. You can resume normal activity. It is advised to unsubscribe/subscribe again all channels.

  def handleEvent(event: String, j: JsObject): Unit = event match {
    case "subscribed" => if (log.isTraceEnabled) log.trace(s"received SubscribeResponse message: $j")
    case "error" => log.error(s"received error message: $j")
    case "info" => log.debug(s"received info message: $j")
    case _ => log.warn(s"received unidentified message: $j")
  }

  def decodeJson(s: String): DecodedMessage = JsonMessage(JsonParser(s).asJsObject)

  def decodeDataArray(s: String): DecodedMessage = {
    val snapshotPattern = "^\\[\\d+\\s*,\\s*\\[\\s*\\[.*".r
    snapshotPattern.findFirstIn(s) match {
      case Some(_) => JsonParser(s).convertTo[RawOrderBookSnapshot]
      case None => JsonParser(s).convertTo[RawOrderBookUpdate]
    }
  }

  val sink: Sink[Message, Future[Done]] = Sink.foreach[Message] {
    case msg: TextMessage =>
      msg.toStrict(config.httpTimeout)
        .map(_.getStrictText)
        .map {
          case s:String if s.startsWith("{") => decodeJson(s)
          case s:String if s.startsWith("[") => decodeDataArray(s)
        }.onComplete {
        case Failure(exception) => log.error(s"Unable to decode expected Json message", exception)
        case Success(v) => v match {
          case j: JsonMessage if j.j.fields.contains("event") => handleEvent(j.j.fields("event").convertTo[String], j.j)
          case j: JsonMessage => log.warn(s"Unhandled JsonMessage received: $j")
          case d: RawOrderBookSnapshot => receiver ! d
          case d: RawOrderBookUpdate => receiver ! d
          // TODO handle heartbeats: '[CHANNEL_ID, "hb"]'
          case _ => log.error(s"unhandled message")
        }
      }
    case msg: Message =>
      log.warn(s"Unexpected kind of Message received: $msg")
  }


  // flow to us
  // emits a Message and then keep the connection open
  val flow: Flow[Message, Message, Promise[Option[Message]]] =
  Flow.fromSinkAndSourceCoupledMat(
    sink,
    Source(List(
      TextMessage(SubscribeRequest(symbol = tradePair.apiSymbol).toJson.compactPrint)
    )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)


  val WebSocketEndpoint: Uri = Uri(s"wss://api-pub.bitfinex.com/ws/2")
  val (upgradeResponse, promise) =
    Http().singleWebSocketRequest(
      WebSocketRequest(WebSocketEndpoint),
      flow)

  val connected: Future[Done.type] = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      log.debug("WebSocket connected")
      Future.successful(Done)
    } else {
      throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
  }

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
// TODO send unsubscribe message to bitfinex on shutdown event