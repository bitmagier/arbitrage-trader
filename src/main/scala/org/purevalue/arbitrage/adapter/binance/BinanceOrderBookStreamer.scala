package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.pattern.pipe
import org.purevalue.arbitrage.OrderBook.{AskUpdate, BidUpdate, OrderBookInitialData, OrderBookUpdate}
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter.GetOrderBookSnapshot
import org.purevalue.arbitrage.{ExchangeConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsonParser, RootJsonFormat}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object BinanceOrderBookStreamer {
  def props(config: ExchangeConfig, tradePair: BinanceTradePair, binanceAdapter:ActorRef, receipient: ActorRef): Props = Props(new BinanceOrderBookStreamer(config, tradePair, binanceAdapter, receipient))
}

class BinanceOrderBookStreamer(config: ExchangeConfig, tradePair: BinanceTradePair, binanceAdapter:ActorRef, receipient: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceOrderBookStreamer])
  implicit val system: ActorSystem = Main.actorSystem

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

  private def toInitialData(r: RawOrderBookSnapshot): OrderBookInitialData = {
    OrderBookInitialData(
      r.bids.map(e => toBidUpdate(e)),
      r.asks.map(e => toAskUpdate(e))
    )
  }

  private def toUpdate(r: RawOrderBookUpdate): OrderBookUpdate = {
    OrderBookUpdate(
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

  override def preStart() {
    log.debug(s"BinanceOrderBookStreamer($tradePair) initializing...")
    orderBookWebSocketFlow = context.actorOf(BinanceOrderBookWebSocketFlow.props(config, tradePair, self))
  }

  override def receive: Receive = {
    case snapshot: RawOrderBookSnapshot =>
      initOrderBook(snapshot)

    case update: RawOrderBookUpdate =>
      handleIncoming(update)
      if (bufferingPhase) {
        binanceAdapter ! GetOrderBookSnapshot(tradePair) // when first update is received we ask for the snapshot
      }
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}