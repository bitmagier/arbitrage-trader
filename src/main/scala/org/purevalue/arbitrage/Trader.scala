package org.purevalue.arbitrage

import akka.actor
import akka.actor.{Actor, Props}
import akka.event.Logging

object Trader {
  def props(): Props = Props(new Trader())
}


class Trader extends Actor {
  private val log = Logging(context.system, this)

  var allExchanges: Map[String, actor.ActorRef] = _

  override def preStart():Unit = {
    allExchanges = Map(
      "binance" -> context.actorOf(Binance.
        props(StaticConfig.exchange("binance")), "binance")
    )
    log.info(s"Initializing exchanges: ${allExchanges.keys}")
  }

  def receive: Receive = {
    case _ =>
  }
}
