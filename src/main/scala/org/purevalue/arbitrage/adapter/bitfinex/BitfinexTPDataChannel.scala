package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.TPDataManager._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexDataChannel.GetBitfinexTradePair
import org.purevalue.arbitrage.{AppConfig, ExchangeConfig, Main, TradePair}
import org.slf4j.LoggerFactory

import scala.concurrent.Await


object BitfinexTPDataChannel {
  def props(config: ExchangeConfig, tradePair: TradePair, bitfinexDataChannel: ActorRef, tradePairDataManager: ActorRef): Props =
    Props(new BitfinexTPDataChannel(config, tradePair, bitfinexDataChannel, tradePairDataManager))
}
class BitfinexTPDataChannel(config: ExchangeConfig, tradePair: TradePair, bitfinexDataChannel:ActorRef, tradePairDataManager: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexTPDataChannel])
  implicit val system: ActorSystem = Main.actorSystem

  private var bitfinexTradePair: BitfinexTradePair = _
  private var orderBookWebSocketFlow: ActorRef = _

  private def toOrderBookInitialData(snapshot: RawOrderBookSnapshotMessage) = {
    val bids = snapshot.values
      .filter(_.count > 0)
      .filter(_.amount > 0)
      .map(e => Bid(e.price, e.amount))
    val asks = snapshot.values
      .filter(_.count > 0)
      .filter(_.amount < 0)
      .map(e => Ask(e.price, -e.amount))
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
  private def toOrderBookUpdate(update: RawOrderBookUpdateMessage): OrderBookUpdate = {
    if (update.value.count > 0) {
      if (update.value.amount > 0)
        OrderBookUpdate(List(Bid(update.value.price, update.value.amount)), List())
      else if (update.value.amount < 0)
        OrderBookUpdate(List(), List(Ask(update.value.price, -update.value.amount)))
      else {
        log.warn(s"undefined update case: $update")
        OrderBookUpdate(List(), List())
      }
    } else if (update.value.count == 0) {
      if (update.value.amount == 1.0d)
        OrderBookUpdate(List(Bid(update.value.price, 0.0d)), List())
      else if (update.value.amount == -1.0d)
        OrderBookUpdate(List(), List(Ask(update.value.price, 0.0d)))
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
    log.debug(s"BitfinexTradePairDataStreamer($tradePair) initializing...")
    implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
    bitfinexTradePair = Await.result((bitfinexDataChannel ? GetBitfinexTradePair(tradePair)).mapTo[BitfinexTradePair], AppConfig.tradeRoom.internalCommunicationTimeout.duration)
    orderBookWebSocketFlow = context.actorOf(BitfinexTradePairBasedWebSockets.props(config, bitfinexTradePair, self))
  }

  override def receive: Receive = {

    case s: RawOrderBookSnapshotMessage =>
      log.debug(s"Initializing OrderBook($tradePair) with received snapshot")
      tradePairDataManager ! toOrderBookInitialData(s)

    case u: RawOrderBookUpdateMessage =>
      tradePairDataManager ! toOrderBookUpdate(u)

    case t: RawTickerMessage =>
      tradePairDataManager ! t.value.toTicker(config.exchangeName, tradePair)

    case Status.Failure(cause) =>
      log.error("Failure received", cause)
  }
}
