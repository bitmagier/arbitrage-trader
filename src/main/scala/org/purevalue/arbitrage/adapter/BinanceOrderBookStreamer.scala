package org.purevalue.arbitrage.adapter

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.pattern.pipe
import org.purevalue.arbitrage.OrderBook.{AskUpdate, BidUpdate, InitialData, Update}
import org.purevalue.arbitrage.{ExchangeConfig, Main, TradePair}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsonParser, RootJsonFormat}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object BinanceOrderBookStreamer {
  def props(config: ExchangeConfig, tradePair: TradePair, receipient: ActorRef): Props = Props(new BinanceOrderBookStreamer(config, tradePair, receipient))
}

class BinanceOrderBookStreamer(config: ExchangeConfig, tradePair: TradePair, receipient: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceOrderBookStreamer])

  implicit val system: ActorSystem = Main.actorSystem

  import system.dispatcher

  private var orderBookWebSocketFlow: ActorRef = _
  private var bufferingPhase: Boolean = true
  private val buffer = ListBuffer[RawOrderBookUpdate]()
  private var lastUpdateEvent: RawOrderBookUpdate = _


  private def cleanupBufferEventsAndSend(snapshotLastUpdateId: Long): Unit = {
    bufferingPhase = false
    val postInitEvents = buffer.filter(_.u > snapshotLastUpdateId)
    if (postInitEvents.nonEmpty) {
      if (!(postInitEvents.head.U <= snapshotLastUpdateId + 1 && postInitEvents.head.u >= snapshotLastUpdateId + 1)) {
        log.warn(s"First Update event after OrderBookSnapshot (${postInitEvents.head}) does not match criteria U <= ${snapshotLastUpdateId + 1} AND u >= ${snapshotLastUpdateId + 1}")
      }
    }
    postInitEvents.foreach(e => receipient ! toUpdate(e))
    buffer.clear()
  }

  private def initOrderBook(snapshot: RawOrderBookSnapshot): Unit = {
    log.debug(s"Initializing OrderBook($tradePair) with received snapshot")
    receipient ! toInitialData(snapshot)
    cleanupBufferEventsAndSend(snapshot.lastUpdateId)
  }


  private def toBidUpdate(e: Seq[String]): BidUpdate = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    BidUpdate(
      e.head.toDouble, // Price level
      e(1).toDouble // Quantity
    )
  }

  private def toAskUpdate(e: Seq[String]): AskUpdate = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    AskUpdate(
      e.head.toDouble, // Price level
      e(1).toDouble // Quantity
    )
  }

  private def toInitialData(r: RawOrderBookSnapshot): InitialData = {
    InitialData(
      r.bids.map(e => toBidUpdate(e)),
      r.asks.map(e => toAskUpdate(e))
    )
  }

  private def toUpdate(r: RawOrderBookUpdate): Update = {
    Update(
      r.b.map(e => toBidUpdate(e)),
      r.a.map(e => toAskUpdate(e))
    )
  }

  def handleIncoming(update: RawOrderBookUpdate): Unit = {
    if (lastUpdateEvent != null && update.U != lastUpdateEvent.u + 1) {
      log.error(s"Update stream inconsistent: First UpdateId of this event ({$update.U}) is NOT last FinalUpdateId (${lastUpdateEvent.u})+1")
    }
    if (bufferingPhase) {
      buffer += update
    } else {
      receipient ! toUpdate(update)
    }
    lastUpdateEvent = update
  }

  def queryOrderBookSnapshot(): Future[RawOrderBookSnapshot] = {
    import RawOrderBookStreamJsonProtokoll._
    log.debug(s"Binance: Get OrderBookSnapshot for $tradePair")

    val responseFuture = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = s"${BinanceAdapter.baseEndpoint}/api/v3/depth?symbol=${tradePair.symbol}&limit=1000"
      ))
    responseFuture
      .flatMap(_.entity.toStrict(config.httpTimeout))
      .map(r => r.contentType match {
        case ContentTypes.`application/json` =>
          try {
            JsonParser(r.data.utf8String).convertTo[RawOrderBookSnapshot]
          } catch {
            case e: Exception => throw new RuntimeException(s"Failed to parse RawOrderBookSnapshot from this json:\n${r.data.utf8String}", e)
          }
        case _ =>
          throw new Exception(s"RawOrderBookSnapshot query response is not a json:\n${r.data.utf8String}")
      })
  }

  override def preStart() {
    log.debug(s"BinanceOrderBookStreamer($tradePair) initializing...")
    orderBookWebSocketFlow = context.actorOf(BinanceOrderBookWebSocketFlow.props(config, tradePair, self))
  }

  override def receive: Receive = {

    case snapshot: RawOrderBookSnapshot =>
      initOrderBook(snapshot)
      log.debug(s"BinanceOrderBookStreamer($tradePair) initialized")

    case update: RawOrderBookUpdate =>
      handleIncoming(update)
      if (bufferingPhase) {
        queryOrderBookSnapshot() pipeTo self
      }
    case Status.Failure(cause) => log.error("received failure", cause)
  }
}

case class RawOrderBookSnapshot(lastUpdateId: Long, bids: Seq[Seq[String]], asks: Seq[Seq[String]])

object RawOrderBookStreamJsonProtokoll extends DefaultJsonProtocol {
  implicit val orderBookSnapshot: RootJsonFormat[RawOrderBookSnapshot] = jsonFormat3(RawOrderBookSnapshot)
}