package org.purevalue.arbitrage

import java.text.DecimalFormat
import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.OrderBookManager.{AskPosition, BidPosition, OrderBookInitialData, OrderBookUpdate}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.OrderBookStreamRequest
import org.slf4j.LoggerFactory

case class OrderBook(tradePair: TradePair, bids: Map[Double, BidPosition], asks: Map[Double, AskPosition], lastUpdated: LocalDateTime) {
  private def formatPrice(d: Double) = new DecimalFormat("#.##########").format(d)

  def toCondensedString: String =
    s"${bids.keySet.size} Bids(max price: ${formatPrice(bids.maxBy(_._2.price)._2.price)}) " +
      s"${asks.keySet.size} Asks(min price: ${formatPrice(asks.minBy(_._2.price)._2.price)})"
}

object OrderBookManager {
  case class Initialized(tradePair: TradePair)
  case class BidPosition(price: Double, qantity: Double) // (likely aggregated) bid position(s) for a price level
  case class AskPosition(price: Double, qantity: Double) // (likely aggregated) ask position(s) for a price level
  case class OrderBookInitialData(bids: Seq[BidPosition], asks: Seq[AskPosition])
  case class OrderBookUpdate(bids: Seq[BidPosition], asks: Seq[AskPosition])

  def props(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, parentActor: ActorRef): Props =
    Props(new OrderBookManager(exchange, tradePair, exchangeQueryAdapter, parentActor))
}

case class OrderBookManager(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, parentActor: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OrderBookManager])
  private var orderBook: OrderBook = OrderBook(tradePair, Map(), Map(), LocalDateTime.MIN)

  override def preStart(): Unit = {
    exchangeQueryAdapter ! OrderBookStreamRequest(tradePair)
  }

  def receive: Receive = {
    case i: OrderBookInitialData =>
      orderBook = OrderBook(
        tradePair,
        i.bids.map(e => Tuple2(e.price, e)).toMap,
        i.asks.map(e => Tuple2(e.price, e)).toMap,
        LocalDateTime.now()
      )
      if (log.isTraceEnabled) log.trace(s"OrderBook $tradePair received initial data")
      parentActor ! OrderBookManager.Initialized(tradePair)

    case u: OrderBookUpdate =>
      val newBids = u.bids.map(e => Tuple2(e.price, e)).toMap
      val newAsks = u.asks.map(e => Tuple2(e.price, e)).toMap
      orderBook = OrderBook(
        tradePair,
        (orderBook.bids ++ newBids).filter(_._2.qantity != 0.0d),
        (orderBook.asks ++ newAsks).filter(_._2.qantity != 0.0d),
        LocalDateTime.now()
      )

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