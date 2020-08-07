package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import org.purevalue.arbitrage.TradepairDataManager.{Ask, Bid, OrderBookInitialData, OrderBookUpdate}
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter.GetOrderBookSnapshot
import org.purevalue.arbitrage.{ExchangeConfig, Main}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object BinanceTPDataChannel {
  def props(config: ExchangeConfig, tradePair: BinanceTradePair, binanceAdapter:ActorRef, tradePairDataManager: ActorRef): Props =
    Props(new BinanceTPDataChannel(config, tradePair, binanceAdapter, tradePairDataManager))
}

class BinanceTPDataChannel(config: ExchangeConfig, tradePair: BinanceTradePair, binanceAdapter:ActorRef, tradePairDataManager: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTPDataChannel])
  implicit val system: ActorSystem = Main.actorSystem

  private var webSocketFlow: ActorRef = _
  private var orderBookBufferingPhase: Boolean = true
  private var orderBookSnapshotRequested: Boolean = false
  private val orderBookBuffer = ListBuffer[RawOrderBookUpdate]()
  private var lastUpdateEvent: RawOrderBookUpdate = _


  private def cleanupBufferEventsAndSend(snapshotLastUpdateId: Long): Unit = {
    orderBookBufferingPhase = false
    val postInitEvents = orderBookBuffer.filter(_.u > snapshotLastUpdateId)
    if (postInitEvents.nonEmpty) {
      if (!(postInitEvents.head.U <= snapshotLastUpdateId + 1 && postInitEvents.head.u >= snapshotLastUpdateId + 1)) {
        log.warn(s"First Update event after OrderBookSnapshot (${postInitEvents.head}) does not match criteria " +
          s"U <= ${snapshotLastUpdateId + 1} AND u >= ${snapshotLastUpdateId + 1}")
      }
    }
    postInitEvents.foreach(e => tradePairDataManager ! toOrderBookUpdate(e))
    orderBookBuffer.clear()
  }

  private def initOrderBook(snapshot: RawOrderBookSnapshot): Unit = {
    log.debug(s"Initializing OrderBook($tradePair) with received snapshot")
    tradePairDataManager ! toInitialData(snapshot)
    cleanupBufferEventsAndSend(snapshot.lastUpdateId)
  }

  private def toBidUpdate(e: Seq[String]): Bid = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    Bid(
      e.head.toDouble, // Price level
      e(1).toDouble // Quantity
    )
  }

  private def toAskUpdate(e: Seq[String]): Ask = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    Ask(
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

  def handleIncomingOrderBookUpdate(update: RawOrderBookUpdate): Unit = {
    if (lastUpdateEvent != null && update.U != lastUpdateEvent.u + 1) {
      log.error(s"Update stream inconsistent: First UpdateId of this event ({$update.U}) is NOT last FinalUpdateId (${lastUpdateEvent.u})+1")
    }
    if (orderBookBufferingPhase) {
      orderBookBuffer += update
    } else {
      tradePairDataManager ! toOrderBookUpdate(update)
    }
    lastUpdateEvent = update
  }

  override def preStart() {
    log.debug(s"BinanceTradePairDataStreamer($tradePair) initializing...")
    webSocketFlow = context.actorOf(BinanceTradePairBasedWebSockets.props(config, tradePair, self))
  }

  override def receive: Receive = {
    case s: RawOrderBookSnapshot =>
      initOrderBook(s)

    // 2020-08-07 00:42:38.277 INFO  akka.actor.LocalActorRef - Message [org.purevalue.arbitrage.adapter.bitfinex.RawOrderBookUpdateMessage]
    // from Actor[akka://ArbitrageTrader/user/TradeRoom/BitfinexAdapter/BitfinexTradePairDataStreamer-BCH:BTC/$a#1542073973]
    // to Actor[akka://ArbitrageTrader/user/TradeRoom/BitfinexAdapter/BitfinexTradePairDataStreamer-BCH:BTC#208834353] was not delivered.
    // [10] dead letters encountered, no more dead letters will be logged in next [5.000 min].
    // If this is not an expected behavior then Actor[akka://ArbitrageTrader/user/TradeRoom/BitfinexAdapter/BitfinexTradePairDataStreamer-BCH:BTC#208834353]
    // may have terminated unexpectedly.
    // This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
    case u: RawOrderBookUpdate =>
      handleIncomingOrderBookUpdate(u)
      if (orderBookBufferingPhase && !orderBookSnapshotRequested) {
        orderBookSnapshotRequested = true
        binanceAdapter ! GetOrderBookSnapshot(tradePair) // when first update is received we ask for the snapshot
      }

    case t:RawTicker =>
      tradePairDataManager ! t.toTicker(config.exchangeName, tradePair)

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO limit order book to about 100 entries - currently we have 1000; Hint: there is a levels-parameter [5,10,20] for the partial book depth stream, but how does it work?