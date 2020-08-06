package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.TradePairDataManager._
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy
import org.slf4j.LoggerFactory


object TradePairDataManager {
  case class GetTicker()
  case class GetOrderBook()
  case class Initialized(tradePair: TradePair)
  case class Bid(price: Double, quantity: Double) { // A bid is an offer to buy an asset; (likely aggregated) bid position(s) for a price level
    override def toString: String = s"Bid(price=${CryptoValue.formatDecimal(price)}, amount=${CryptoValue.formatDecimal(quantity)})"
  }
  case class Ask(price: Double, quantity: Double) { // An ask is an offer to sell an asset; (likely aggregated) ask position(s) for a price level
    override def toString: String = s"Ask(price=${CryptoValue.formatDecimal(price)}, amount=${CryptoValue.formatDecimal(quantity)})"
  }
  case class OrderBookInitialData(bids: Seq[Bid], asks: Seq[Ask])
  case class OrderBookUpdate(bids: Seq[Bid], asks: Seq[Ask])

  def props(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, exchangeActor: ActorRef): Props =
    Props(new TradePairDataManager(exchange, tradePair, exchangeQueryAdapter, exchangeActor))
}

/**
 * Manages all kind of data of one tradepair at one exchange
 */
case class TradePairDataManager(exchange: String, tradePair: TradePair, exchangeQueryAdapter: ActorRef, exchangeActor: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradePairDataManager])
  private var initializedMsgSend = false
  private var orderBookInitialized = false
  private var orderBook: OrderBook = OrderBook(exchange, tradePair, Map(), Map(), LocalDateTime.MIN)
  private var ticker: Option[Ticker] = None

  def initialized: Boolean = orderBookInitialized && ticker.isDefined

  def eventuallyInitialized(): Unit = {
    if (!initializedMsgSend && initialized) {
      initializedMsgSend = true
      exchangeActor ! Initialized(tradePair)
    }
  }

  override def preStart(): Unit = {
    exchangeQueryAdapter ! ExchangeAdapterProxy.TradePairDataStreamRequest(tradePair)
  }

  def receive: Receive = {

    // Messages from Exchange

    case GetTicker() =>
      if (initialized)
        sender() ! ticker.get

    case GetOrderBook() =>
      if (initialized)
        sender() ! orderBook


    // Messages from TradePairBasedDataStreamer

    case t: Ticker =>
      ticker = Some(t)
      eventuallyInitialized()

    case i: OrderBookInitialData =>
      orderBook = OrderBook(
        exchange,
        tradePair,
        i.bids.map(e => Tuple2(e.price, e)).toMap,
        i.asks.map(e => Tuple2(e.price, e)).toMap,
        LocalDateTime.now())
      if (log.isTraceEnabled) log.trace(s"OrderBook $tradePair received initial data")
      orderBookInitialized = true
      eventuallyInitialized()

    case u: OrderBookUpdate =>
      val newBids = u.bids.map(e => Tuple2(e.price, e)).toMap
      val newAsks = u.asks.map(e => Tuple2(e.price, e)).toMap
      orderBook = OrderBook(
        exchange,
        tradePair,
        (orderBook.bids ++ newBids).filter(_._2.quantity != 0.0d),
        (orderBook.asks ++ newAsks).filter(_._2.quantity != 0.0d),
        LocalDateTime.now())

      if (log.isTraceEnabled) {
        log.trace(s"OrderBook $exchange:$tradePair received update. $status")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }

  def status: String = {
    orderBook.toCondensedString
  }
}