package org.purevalue.arbitrage

import akka.actor.ActorSystem

object Main extends App {
  lazy val actorSystem = ActorSystem("ArbitrageTrader")
  val trader = actorSystem.actorOf(Trader.props(), "trader")
}
