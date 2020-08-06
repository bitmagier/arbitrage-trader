package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.TradePairDataManager._
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.TradePairBasedDataStreamRequest
import org.slf4j.LoggerFactory


object TradePairDataManager {
  case class GetTicker()
  case class GetOrderBook()
  case class Initialized(tradePair: TradePair)
  case class BidPosition(price: Double, qantity: Double) // (likely aggregated) bid position(s) for a price level
  case class AskPosition(price: Double, qantity: Double) // (likely aggregated) ask position(s) for a price level
  case class OrderBookInitialData(bids: Seq[BidPosition], asks: Seq[AskPosition])
  case class OrderBookUpdate(bids: Seq[BidPosition], asks: Seq[AskPosition])
  case class ExchangeHeartbeat(ts:LocalDateTime)

  def props(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, parentActor: ActorRef): Props =
    Props(new TradePairDataManager(exchange, tradePair, exchangeQueryAdapter, parentActor))
}

/**
 * Manages all kind of data of one tradepair at one exchange
 */
case class TradePairDataManager(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, parentActor: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradePairDataManager])
  private var orderBook: OrderBook = OrderBook(exchange, tradePair, Map(), Map(), LocalDateTime.MIN)
  private var ticker: Ticker = _

  override def preStart(): Unit = {
    exchangeQueryAdapter ! TradePairBasedDataStreamRequest(tradePair)
  }

  def receive: Receive = {

    // Messages from Exchange

    case GetTicker() => sender() ! ticker
    case GetOrderBook() => sender() ! orderBook


    // Messages from TradePairBasedDataStreamer
//    case ExchangeHeartbeat(ts) =>
//      orderBook = OrderBook(
//        exchange,
//        tradePair,
//        orderBook.bids,
//        orderBook.asks,
//        ts)

    case t: Ticker =>
      ticker = t

    case i: OrderBookInitialData =>
      orderBook = OrderBook(
        exchange,
        tradePair,
        i.bids.map(e => Tuple2(e.price, e)).toMap,
        i.asks.map(e => Tuple2(e.price, e)).toMap,
        LocalDateTime.now())
      if (log.isTraceEnabled) log.trace(s"OrderBook $tradePair received initial data")
      parentActor ! TradePairDataManager.Initialized(tradePair)

    case u: OrderBookUpdate =>
      val newBids = u.bids.map(e => Tuple2(e.price, e)).toMap
      val newAsks = u.asks.map(e => Tuple2(e.price, e)).toMap
      orderBook = OrderBook(
        exchange,
        tradePair,
        (orderBook.bids ++ newBids).filter(_._2.qantity != 0.0d),
        (orderBook.asks ++ newAsks).filter(_._2.qantity != 0.0d),
        LocalDateTime.now())

      if (log.isTraceEnabled) {
        log.trace(s"OrderBook $exchange:$tradePair received update. $status")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }

  def status: String = {
    orderBook.toCondensedString
  }
}