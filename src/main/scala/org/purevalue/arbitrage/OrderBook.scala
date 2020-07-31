package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props}
import org.purevalue.arbitrage.OrderBook.{AskUpdate, BidUpdate, InitialData, Update}
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.OrderBookStreamRequest
import org.slf4j.LoggerFactory


object OrderBook {
  case class Initialized(tradePair:TradePair)
  case class BidUpdate(price:Double, qantity:Double)
  case class AskUpdate(price:Double, qantity:Double)
  case class InitialData(bids: Seq[BidUpdate], asks: Seq[AskUpdate])
  final case class Update(bids: Seq[BidUpdate], asks: Seq[AskUpdate])

  def props(exchange:String, tradePair:TradePair, exchangeQueryAdapter:ActorRef, parentActor:ActorRef): Props = Props(new OrderBook(exchange, tradePair, exchangeQueryAdapter, parentActor))
}

case class OrderBook(exchange:String, tradePair:TradePair, exchangeQueryAdapter:ActorRef, parentActor:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OrderBook])
  var lastUpdated: LocalDateTime = _
  var bids:List[BidUpdate] = _
  var asks:List[AskUpdate] = _

  override def preStart(): Unit = {
    super.preStart()
    exchangeQueryAdapter ! OrderBookStreamRequest(tradePair)
  }

  def receive: Receive = {
    case i:InitialData =>
      bids = i.bids.sortBy(_.price).toList
      asks = i.asks.sortBy(_.price).toList
      lastUpdated = LocalDateTime.now()
      parentActor ! OrderBook.Initialized(tradePair)
      log.debug(s"OrderBook $tradePair received initial data")

    case u:Update =>
      val newBids = u.bids.map(_.price).toSet
      val newAsks = u.asks.map(_.price).toSet
      bids = (bids.filterNot(e => newBids.contains(e.price)) ++ u.bids).filter(_.qantity != 0.0d).sortBy(_.price)
      asks = (asks.filterNot(e => newAsks.contains(e.price)) ++ u.asks).filter(_.qantity != 0.0d).sortBy(_.price)
      lastUpdated = LocalDateTime.now()
      log.debug(s"OrderBook $tradePair received update")
  }
}