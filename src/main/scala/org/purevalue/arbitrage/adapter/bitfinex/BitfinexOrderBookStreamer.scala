package org.purevalue.arbitrage.adapter.bitfinex

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import org.purevalue.arbitrage.OrderBookManager._
import org.purevalue.arbitrage.{ExchangeConfig, Main}
import org.slf4j.LoggerFactory


object BitfinexOrderBookStreamer {
  def props(config: ExchangeConfig, tradePair: BitfinexTradePair, receipient: ActorRef): Props =
    Props(new BitfinexOrderBookStreamer(config, tradePair, receipient))
}
class BitfinexOrderBookStreamer(config: ExchangeConfig, tradePair: BitfinexTradePair, receipient: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexOrderBookStreamer])
  implicit val system: ActorSystem = Main.actorSystem

  private var orderBookWebSocketFlow: ActorRef = _

  private def toOrderBookInitialData(snapshot: RawOrderBookSnapshot) = {
    val bids = snapshot.values
      .filter(_.count > 0)
      .filter(_.amount > 0)
      .map(e => BidPosition(e.price, e.amount))
    val asks = snapshot.values
      .filter(_.count > 0)
      .filter(_.amount < 0)
      .map(e => AskPosition(e.price, -e.amount))
    OrderBookInitialData(
      bids, asks
    )
  }

  /*
    Algorithm to create and keep a book instance updated

    1. subscribe to channel
    2. receive the book snapshot and create your in-memory book structure
    3. when count > 0 then you have to add or update the price level
    3.1 if amount > 0 then add/update bids
    3.2 if amount < 0 then add/update asks
    4. when count = 0 then you have to delete the price level.
    4.1 if amount = 1 then remove from bids
    4.2 if amount = -1 then remove from asks
  */
  private def toOrderBookUpdate(update: RawOrderBookUpdate): OrderBookUpdate = {
    if (update.value.count > 0) {
      if (update.value.amount > 0)
        OrderBookUpdate(List(BidPosition(update.value.price, update.value.amount)), List())
      else if (update.value.amount < 0)
        OrderBookUpdate(List(), List(AskPosition(update.value.price, -update.value.amount)))
      else {
        log.warn(s"undefined update case: $update")
        OrderBookUpdate(List(), List())
      }
    } else if (update.value.count == 0) {
      if (update.value.amount == 1.0d)
        OrderBookUpdate(List(BidPosition(update.value.price, 0.0d)), List())
      else if (update.value.amount == -1.0d)
        OrderBookUpdate(List(), List(AskPosition(update.value.price, 0.0d)))
      else {
        log.warn(s"undefined update case: $update")
        OrderBookUpdate(List(), List())
      }
    } else {
      log.warn(s"undefined update case: $update")
      OrderBookUpdate(List(), List())
    }
  }

  override def preStart() {
    log.debug(s"BitfinexBookStreamer($tradePair) initializing...")
    orderBookWebSocketFlow = context.actorOf(BitfinexOrderBookWebSocketFlow.props(config, tradePair, self))
  }

  override def receive: Receive = {
    case h: Heartbeat =>
      receipient ! OrderBookHeartbeat(LocalDateTime.now())

    case snapshot: RawOrderBookSnapshot =>
      log.debug(s"Initializing OrderBook($tradePair) with received snapshot")
      receipient ! toOrderBookInitialData(snapshot)

    case update: RawOrderBookUpdate =>
      receipient ! toOrderBookUpdate(update)

    case Status.Failure(cause) =>
      log.error("Failure received", cause)
  }
}
