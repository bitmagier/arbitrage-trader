package org.purevalue.arbitrage

import akka.actor
import akka.actor.{Actor, Props}
import akka.event.Logging

object Trader {
  def props(): Props = Props(new Trader())
}


class Trader extends Actor {
  private val log = Logging(context.system, this)

  var workingExchanges:Set[String] = Set[String]()
  lazy val allExchanges: Map[String, actor.ActorRef] = Map(
    "binance" -> context.actorOf(Binance.props(StaticConfig.exchange("binance")), "binance")
  )

  override def preStart():Unit = {
    log.info(s"Initializing exchanges: ${allExchanges.keys}")
    allExchanges.values.foreach(_ ! Exchange.Init)
  }

  def receive: Receive = {
    case Exchange.Initialized(e) =>
      workingExchanges += e
      log.info(s"Working exchanges: $workingExchanges")
  }
}
