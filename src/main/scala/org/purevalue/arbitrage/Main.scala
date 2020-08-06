package org.purevalue.arbitrage

import akka.actor.ActorSystem

object Main extends App {
  lazy val actorSystem = ActorSystem("ArbitrageTrader")
  val tradeRoom = actorSystem.actorOf(TradeRoom.props(StaticConfig.tradeRoom), "TradeRoom")
}

// TODO reduce binance order book