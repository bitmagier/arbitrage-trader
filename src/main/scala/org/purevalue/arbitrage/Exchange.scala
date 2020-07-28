package org.purevalue.arbitrage
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.{Init, Initialized, TradePairs}
import org.purevalue.arbitrage.adapter.BinanceAdapter
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.GetTradePairs

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class TradePair(symbol:String, baseAsset:String, quoteAsset:String)
case class Fee(makerFee:Double, takerFee:Double)

object Exchange {
  case class Init()
  case class Initialized(name:String)
  case class TradePairs(pairs: Seq[TradePair])
}

abstract class Exchange extends Actor {
  private val log = Logging(context.system, this)
  // static
  def name:String
  def assets:Set[String]
  private val orderBookInitTimeout = java.time.Duration.ofSeconds(15)
  def fee:Fee
  // dynamic
  var tradePairs: Set[TradePair] = _
  var orderBooks: mutable.Map[TradePair, ActorRef] = _
  var orderBookInitPending:Set[TradePair] = _

  def exchangeAdapter:ActorRef

  def initOrderBooks(): Unit = {
    orderBooks = mutable.Map[TradePair, ActorRef]()
    for (p <- tradePairs) {
      orderBooks += (p -> context.actorOf(OrderBook.props(name, p, exchangeAdapter), s"$name.OrderBook-$p"))
    }
    orderBookInitPending = orderBooks.keySet.toSet
    orderBooks.values.foreach(b => b ! OrderBook.Init)
    val orderBookInitStartTime = Instant.now()
    while (orderBookInitPending.nonEmpty
      && orderBookInitStartTime.plus(orderBookInitTimeout).isAfter(Instant.now())) {
      Thread.sleep(100)
    }
    if (orderBookInitPending.nonEmpty) throw new Exception(s"Timeout while waiting for $name OrderBooks[$orderBookInitPending] initialization")
  }

  def initTradePairs():Unit = {
    implicit val timeout: Timeout = Timeout(10, TimeUnit.SECONDS)
    val awaitTimeout:Duration = Duration(1, TimeUnit.SECONDS)
    tradePairs = Await.result((exchangeAdapter ? GetTradePairs()).mapTo[TradePairs], awaitTimeout).pairs.toSet
  }

  def initDynamicParts(): Unit = {
    initTradePairs()
    initOrderBooks()
  }

  def initialized: Boolean = tradePairs != null

  override def receive: Receive = {
    case Init =>
      log.info(s"Initializing exchange $name")
      initDynamicParts()
      sender() ! Initialized(name)

    case OrderBook.Initialized(t) =>
      orderBookInitPending -= t
  }
}

object Binance {
  def props(config:ExchangeConfig): Props = Props(new Binance(config))
}

class Binance(val config:ExchangeConfig) extends Exchange {
  private val log = Logging(context.system, this)
  val name = "binance"
  val assets: Set[String] = config.assets
  val fee: Fee = Fee(config.makerFee, config.takerFee)
  val exchangeAdapter: ActorRef = context.actorOf(Props[BinanceAdapter], "BinanceAdapter")
}