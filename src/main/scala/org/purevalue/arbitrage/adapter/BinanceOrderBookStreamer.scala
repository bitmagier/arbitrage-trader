package org.purevalue.arbitrage.adapter

import akka.actor.{Actor, ActorRef, Props}
import org.purevalue.arbitrage.TradePair

object BinanceOrderBookStreamer {
  def props(tradePair:TradePair, receipient:ActorRef):Props = Props(new BinanceOrderBookStreamer(tradePair, receipient))
}

class BinanceOrderBookStreamer(tradePair:TradePair, receipient:ActorRef) extends Actor {
  override def receive: Receive = {
    case _ => // TODO
  }
}
