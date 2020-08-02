package org.purevalue.arbitrage

import java.text.DecimalFormat
import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.OrderBook.{AskUpdate, BidUpdate, OrderBookInitialData, OrderBookUpdate}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.OrderBookStreamRequest
import org.slf4j.LoggerFactory

object OrderBook {
  case class Initialized(tradePair: TradePair)
  case class BidUpdate(price: Double, qantity: Double)
  case class AskUpdate(price: Double, qantity: Double)
  case class OrderBookInitialData(bids: Seq[BidUpdate], asks: Seq[AskUpdate])
  case class OrderBookUpdate(bids: Seq[BidUpdate], asks: Seq[AskUpdate])

  def props(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, parentActor: ActorRef): Props =
    Props(new OrderBook(exchange, tradePair, exchangeQueryAdapter, parentActor))
}

case class OrderBook(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, parentActor: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OrderBook])
  var lastUpdated: LocalDateTime = _
  var bids: Map[Double, BidUpdate] = _ // key is the price
  var asks: Map[Double, AskUpdate] = _ // key is the price

  override def preStart(): Unit = {
    exchangeQueryAdapter ! OrderBookStreamRequest(tradePair)
  }

  def receive: Receive = {
    case i: OrderBookInitialData =>
      bids = i.bids.map(e => Tuple2(e.price, e)).toMap
      asks = i.asks.map(e => Tuple2(e.price, e)).toMap
      lastUpdated = LocalDateTime.now()
      if (log.isTraceEnabled) log.trace(s"OrderBook $tradePair received initial data")
      parentActor ! OrderBook.Initialized(tradePair)

    case u: OrderBookUpdate =>
      val newBids = u.bids.map(e => Tuple2(e.price, e)).toMap
      val newAsks = u.asks.map(e => Tuple2(e.price, e)).toMap
      bids = (bids ++ newBids).filter(_._2.qantity != 0.0d)
      asks = (asks ++ newAsks).filter(_._2.qantity != 0.0d)
      lastUpdated = LocalDateTime.now()
      if (log.isTraceEnabled) {
        log.trace(s"OrderBook $exchange:$tradePair received update. $status")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }

  private def formatPrice(d:Double) = new DecimalFormat("#.##########").format(d)

  def status: String = {
    s"${bids.keySet.size} Bids(max price: ${formatPrice(bids.maxBy(_._2.price)._2.price)}) " +
      s"${asks.keySet.size} Asks(min price: ${formatPrice(asks.minBy(_._2.price)._2.price)})"
  }
}