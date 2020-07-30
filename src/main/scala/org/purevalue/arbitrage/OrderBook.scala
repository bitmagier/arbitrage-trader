package org.purevalue.arbitrage


import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import org.purevalue.arbitrage.OrderBook._
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.OrderBookStreamRequest


object OrderBook {
  case class Initialized(tradePair:TradePair)
  case class BidUpdate(price:Float, qantity:Float)
  case class AskUpdate(price:Float, qantity:Float)
  case class InitialData(bids: Seq[BidUpdate], asks: Seq[AskUpdate])
  final case class Update(bids: Seq[BidUpdate], asks: Seq[AskUpdate])

  def props(exchange:String, tradePair:TradePair, exchangeQueryAdapter:ActorRef, parentActor:ActorRef): Props = Props(new OrderBook(exchange, tradePair, exchangeQueryAdapter, parentActor))
}

class OrderBook(val exchange:String, val tradePair:TradePair, exchangeQueryAdapter:ActorRef, parentActor:ActorRef) extends Actor {
  private val log = Logging(context.system, this)
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

    case u:Update =>
      val newBids = u.bids.map(_.price).toSet
      val newAsks = u.asks.map(_.price).toSet
      bids = (bids.filterNot(e => newBids.contains(e.price)) ++ u.bids).sortBy(_.price)
      asks = (asks.filterNot(e => newAsks.contains(e.price)) ++ u.asks).sortBy(_.price)
      lastUpdated = LocalDateTime.now()
  }
}