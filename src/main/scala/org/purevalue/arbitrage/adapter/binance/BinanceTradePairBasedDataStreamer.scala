package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import org.purevalue.arbitrage.TradePairDataManager.{AskPosition, BidPosition, OrderBookInitialData, OrderBookUpdate}
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter.GetOrderBookSnapshot
import org.purevalue.arbitrage.{ExchangeConfig, Main}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object BinanceTradePairBasedDataStreamer {
  def props(config: ExchangeConfig, tradePair: BinanceTradePair, binanceAdapter:ActorRef, receipient: ActorRef): Props =
    Props(new BinanceTradePairBasedDataStreamer(config, tradePair, binanceAdapter, receipient))
}

class BinanceTradePairBasedDataStreamer(config: ExchangeConfig, tradePair: BinanceTradePair, binanceAdapter:ActorRef, receipient: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTradePairBasedDataStreamer])
  implicit val system: ActorSystem = Main.actorSystem

  private var webSocketFlow: ActorRef = _
  private var bufferingPhase: Boolean = true
  private var orderBookSnapshotRequested: Boolean = false
  private val buffer = ListBuffer[RawOrderBookUpdate]()
  private var lastUpdateEvent: RawOrderBookUpdate = _


  private def cleanupBufferEventsAndSend(snapshotLastUpdateId: Long): Unit = {
    bufferingPhase = false
    val postInitEvents = buffer.filter(_.u > snapshotLastUpdateId)
    if (postInitEvents.nonEmpty) {
      if (!(postInitEvents.head.U <= snapshotLastUpdateId + 1 && postInitEvents.head.u >= snapshotLastUpdateId + 1)) {
        log.warn(s"First Update event after OrderBookSnapshot (${postInitEvents.head}) does not match criteria " +
          s"U <= ${snapshotLastUpdateId + 1} AND u >= ${snapshotLastUpdateId + 1}")
      }
    }
    postInitEvents.foreach(e => receipient ! toOrderBookUpdate(e))
    buffer.clear()
  }

  private def initOrderBook(snapshot: RawOrderBookSnapshot): Unit = {
    log.debug(s"Initializing OrderBook($tradePair) with received snapshot")
    receipient ! toInitialData(snapshot)
    cleanupBufferEventsAndSend(snapshot.lastUpdateId)
  }

  private def toBidUpdate(e: Seq[String]): BidPosition = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    BidPosition(
      e.head.toDouble, // Price level
      e(1).toDouble // Quantity
    )
  }

  private def toAskUpdate(e: Seq[String]): AskPosition = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    AskPosition(
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

  private def toOrderBookUpdate(r: RawOrderBookUpdate): OrderBookUpdate = {
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
      receipient ! toOrderBookUpdate(update)
    }
    lastUpdateEvent = update
  }

  override def preStart() {
    log.debug(s"BinanceOrderBookStreamer($tradePair) initializing...")
    webSocketFlow = context.actorOf(BinanceTradePairBasedWebSockets.props(config, tradePair, self))
  }

  override def receive: Receive = {
    case s: RawOrderBookSnapshot =>
      initOrderBook(s)

    case u: RawOrderBookUpdate =>
      handleIncoming(u)
      if (bufferingPhase && !orderBookSnapshotRequested) {
        orderBookSnapshotRequested = true
        binanceAdapter ! GetOrderBookSnapshot(tradePair) // when first update is received we ask for the snapshot
      }

    case t:RawTicker =>
      receipient ! t.toTicker(BinanceAdapter.exchangeName, tradePair)

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO limit order book to about 100 entries - currently we have 1000; Hint: there is a levels-parameter [5,10,20] for the partial book depth stream, but how does it work?