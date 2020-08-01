package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props}
import org.purevalue.arbitrage.OrderBook.{AskUpdate, BidUpdate, InitialData, Update}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.OrderBookStreamRequest
import org.slf4j.LoggerFactory


object OrderBook {
  case class Initialized(tradePair:TradePair)
  case class BidUpdate(key:String, price:Double, qantity:Double)
  case class AskUpdate(key:String, price:Double, qantity:Double)
  case class InitialData(bids: Seq[BidUpdate], asks: Seq[AskUpdate])
  final case class Update(bids: Seq[BidUpdate], asks: Seq[AskUpdate])

  def props(exchange:String, tradePair:TradePair, exchangeQueryAdapter:ActorRef, parentActor:ActorRef): Props =
    Props(new OrderBook(exchange, tradePair, exchangeQueryAdapter, parentActor))
}

case class OrderBook(exchange:String, tradePair:TradePair, exchangeQueryAdapter:ActorRef, parentActor:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OrderBook])
  var lastUpdated: LocalDateTime = _
  var bids:Map[String, BidUpdate] = _
  var asks:Map[String, AskUpdate] = _

  override def preStart(): Unit = {
    exchangeQueryAdapter ! OrderBookStreamRequest(tradePair)
  }

  def receive: Receive = {
    case i:InitialData =>
      bids = i.bids.map(e => Tuple2(e.key, e)).toMap
      asks = i.asks.map(e => Tuple2(e.key, e)).toMap
      lastUpdated = LocalDateTime.now()
      if (log.isTraceEnabled) log.trace(s"OrderBook $tradePair received initial data")
      parentActor ! OrderBook.Initialized(tradePair)

    case u:Update =>
      val newBids = u.bids.map(e => Tuple2(e.key, e)).toMap
      val newAsks = u.asks.map(e => Tuple2(e.key, e)).toMap
      bids = (bids ++ newBids).filter(_._2.qantity != 0.0d)
      asks = (asks ++ newAsks).filter(_._2.qantity != 0.0d)
      lastUpdated = LocalDateTime.now()
      if (log.isTraceEnabled) {
        log.trace(s"OrderBook $tradePair received update. $status")
      }
  }

  def status:String = {
    s"${bids.keySet.size} Bids(max price: ${bids.maxBy(_._2.price)._2.price}) " +
      s"${asks.keySet.size} Asks(min price: ${asks.minBy(_._2.price)._2.price})"
  }
}