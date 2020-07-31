package org.purevalue.arbitrage

import akka.actor
import akka.actor.{Actor, Props}
import org.purevalue.arbitrage.adapter.BinanceAdapter
import org.slf4j.LoggerFactory

object Trader {
  def props(): Props = Props(new Trader())
}

class Trader extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Trader])

  var allExchanges: Map[String, actor.ActorRef] = _

  override def preStart(): Unit = {
    log.debug("checking debug logging")
    allExchanges = Map(
      "binance" -> context.actorOf(Exchange.props("binance",
        StaticConfig.exchange("binance"),
        context.actorOf(BinanceAdapter.props(StaticConfig.exchange("binance")), "BinanceAdapter")), "binance"
      )
    )
    log.info(s"Initializing exchanges: ${allExchanges.keys}")
  }

  def receive: Receive = {
    case null =>
  }
}
