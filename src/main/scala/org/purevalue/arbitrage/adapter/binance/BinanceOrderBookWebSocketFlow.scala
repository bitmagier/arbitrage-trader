package org.purevalue.arbitrage.adapter.binance

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.stream.scaladsl._
import org.purevalue.arbitrage.{ExchangeConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.{Future, Promise}

object BinanceOrderBookWebSocketFlow {
  def props(config: ExchangeConfig, tradePair: BinanceTradePair, receiver: ActorRef): Props =
    Props(new BinanceOrderBookWebSocketFlow(config, tradePair, receiver))
}

case class BinanceOrderBookWebSocketFlow(config: ExchangeConfig, tradePair: BinanceTradePair, receiver: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceOrderBookWebSocketFlow])
  private val symbol = tradePair.symbol.toLowerCase()
  implicit val actorSystem: ActorSystem = Main.actorSystem

  import WebSocketJsonProtocoll._
  import actorSystem.dispatcher

  private def handleSubscribeResponse(msg: SubscribeResponse): Unit = {
    if (log.isTraceEnabled) log.trace(s"received SubscribeResponse message: $msg")
  }

  private def handleDepthUpdate(u: RawOrderBookUpdate): Unit = {
    if (log.isTraceEnabled) log.trace(s"received OrderBook update: $u")
    val forward = u match {
      case x if x.e == "depthUpdate" && x.s == tradePair.symbol => x
      case x@_ => throw new RuntimeException(s"RawOrderBookUpdate contained something else than a 'depthUpdate' for '$tradePair'. Here it is: $x")
    }
    receiver ! forward
  }

  val sink: Sink[Message, Future[Done]] = Sink.foreach[Message] {
    case msg: TextMessage =>
      msg.toStrict(config.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject())
        .map {
          case j if j.fields.contains("result") => j.convertTo[SubscribeResponse]
          case j if j.fields.contains("e") && j.fields("e").convertTo[String] == "depthUpdate" => j.convertTo[RawOrderBookUpdate]
          case x: JsObject => throw new RuntimeException(s"Unknown json message received: $x")
        } map {
        case m: SubscribeResponse => handleSubscribeResponse(m)
        case m: RawOrderBookUpdate => handleDepthUpdate(m)
      }
    case x@_ => log.warn(s"Received non TextMessage: $x")
  }

  // flow to us
  // emits a Message and then keep the connection open
  val flow: Flow[Message, Message, Promise[Option[Message]]] =
  Flow.fromSinkAndSourceCoupledMat(
    sink,
    Source(List(
      TextMessage(StreamSubscribeRequest(params = Seq(s"$symbol@depth"), id = 1).toJson.compactPrint)
    )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)


  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/ws/$symbol@depth") // TODO add "@100ms" and solve akka.stream.BufferOverflowException: Exceeded configured max-open-requests value of [32]
  val (upgradeResponse, promise) =
    Http().singleWebSocketRequest(
      WebSocketRequest(WebSocketEndpoint),
      flow)

  // just like a regular http request we can access response status which is available via upgrade.response.status
  // status code 101 (Switching Protocols) indicates that server support WebSockets
  val connected: Future[Done.type] = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      log.debug("WebSocket connected")
      Future.successful(Done)
    } else {
      throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
  }

  // to disconnect call:
  //promise.success(None)

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

case class StreamSubscribeRequest(method: String = "SUBSCRIBE", params: Seq[String], id: Int)
case class SubscribeResponse(result: JsValue, id: Int)
case class RawOrderBookUpdate(e: String /* depthUpdate */ , E: Long /* event time */ , s: String /* symbol */ ,
                              U: Long /* first update ID in event */ ,
                              u: Long /* final update ID in event */ ,
                              b: Seq[Seq[String]],
                              a: Seq[Seq[String]])

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[StreamSubscribeRequest] = jsonFormat3(StreamSubscribeRequest)
  implicit val subscribeResponseMsg: RootJsonFormat[SubscribeResponse] = jsonFormat2(SubscribeResponse)
  implicit val bookUpdate: RootJsonFormat[RawOrderBookUpdate] = jsonFormat7(RawOrderBookUpdate)
}