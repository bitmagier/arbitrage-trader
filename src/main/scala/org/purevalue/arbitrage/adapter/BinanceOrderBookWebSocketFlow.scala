package org.purevalue.arbitrage.adapter

import akka.actor.{Actor, ActorRef, ActorSystem, Kill, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.{Main, TradePair}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.Future

object BinanceOrderBookWebSocketFlow {
  def props(tradePair: TradePair, receiver: ActorRef): Props = Props(new BinanceOrderBookWebSocketFlow(tradePair, receiver))
}

case class BinanceOrderBookWebSocketFlow(tradePair: TradePair, receiver: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceOrderBookWebSocketFlow])
  private val symbol = tradePair.symbol.toLowerCase()
  implicit val actorSystem: ActorSystem = Main.actorSystem

  import WebSocketJsonProtocoll._
  import actorSystem.dispatcher

  private def handleSubscribeResponse(msg: SubscribeResponseMsg): Unit = {
    log.info(s"received SubscribeResponse message: $msg")
  }

  private def handleDepthUpdate(u: RawOrderBookUpdate):Unit = {
    log.debug(s"received OrderBook update: $u")
    if ((u.e != "depthUpdate") || (u.s != tradePair.symbol)) {
      log.warn(s"OrderBookStream contained something else than a 'dephUpdate' for '$tradePair'. Here it is: $u")
    } else {
      receiver ! u
    }
  }

  // Future[Done] is the materialized value of Sink.foreach,
  // emitted when the stream completes
  val incoming: Sink[Message, Future[Done]] = Sink.foreach[Message] {
    case message: TextMessage.Strict =>
      val json = JsonParser(message.text).asJsObject
      if (json.fields.contains("result")) {
        handleSubscribeResponse(json.convertTo[SubscribeResponseMsg])
      } else if (json.fields.contains("e") && json.fields("e").convertTo[String] == "depthUpdate") {
        handleDepthUpdate(json.convertTo[RawOrderBookUpdate])
      } else {
        log.warn(s"Unknown message received: ${message.text}")
      }
  }

  val outgoing: Source[TextMessage.Strict, NotUsed] = Source.single(TextMessage(
    SubscribeMsg(params = Seq(s"$symbol@depth"), id = 1).toJson.compactPrint))

  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/ws/${tradePair.symbol.toLowerCase()}@depth") // TODO +"@100ms"
  // flow to use (note: not re-usable!)
  var webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(WebSocketEndpoint))

  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that
  // completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
  val (upgradeResponse, closed) =
  outgoing
    .viaMat(webSocketFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
    .toMat(incoming)(Keep.both) // also keep the Future[Done]
    .run()

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

  connected.onComplete { _ =>
    log.info("Connection closed. Destroying actor.")
    self ! Kill
  }

  override def receive: Receive = {
    case null =>
  }
}

case class SubscribeMsg(method: String = "SUBSCRIBE", params: Seq[String], id: Int)
case class SubscribeResponseMsg(result: Boolean, id: Int)
case class RawOrderBookUpdate(e: String /* depthUpdate */ , E: Long /* event time */ , s: String /* symbol */ ,
                              U: Long /* first update ID in event */ ,
                              u: Long /* final update ID in event */ ,
                              b: Seq[RawOrderBookEntry],
                              a: Seq[RawOrderBookEntry])

object WebSocketJsonProtocoll extends DefaultJsonProtocol {
  implicit val subscribeMsg: RootJsonFormat[SubscribeMsg] = jsonFormat3(SubscribeMsg)
  implicit val subscribeResponseMsg: RootJsonFormat[SubscribeResponseMsg] = jsonFormat2(SubscribeResponseMsg)
  implicit val entryUpdate: RootJsonFormat[RawOrderBookEntry] = jsonFormat1(RawOrderBookEntry)
  implicit val bookUpdate: RootJsonFormat[RawOrderBookUpdate] = jsonFormat7(RawOrderBookUpdate)
}