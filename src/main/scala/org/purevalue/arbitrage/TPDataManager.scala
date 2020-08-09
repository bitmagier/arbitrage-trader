package org.purevalue.arbitrage

import java.time.{Duration, Instant, LocalDateTime}

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, Props, Status}
import akka.stream.scaladsl.Sink
import org.purevalue.arbitrage.TPDataManager._
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

trait TPStreamData
case class Ticker(exchange: String,
                  tradePair: TradePair,
                  highestBidPrice: Double,
                  highestBidQuantity: Option[Double],
                  lowestAskPrice: Double,
                  lowestAskQuantity: Option[Double],
                  lastPrice: Option[Double],
                  lastUpdated: LocalDateTime) extends TPStreamData
case class ExtendedTicker(exchange: String,
                          tradePair: TradePair,
                          highestBidPrice: Double,
                          highestBidQuantity: Double,
                          lowestAskPrice: Double,
                          lowestAskQuantity: Double,
                          lastPrice: Double,
                          lastQuantity: Double,
                          weightedAveragePrice: Double,
                          lastUpdated: LocalDateTime) extends TPStreamData
case class OrderBookSnapshot(bids: Seq[Bid], asks: Seq[Ask]) extends TPStreamData
case class OrderBookUpdate(bids: Seq[Bid], asks: Seq[Ask]) extends TPStreamData


case class Bid(price: Double, quantity: Double) { // A bid is an offer to buy an asset; (likely aggregated) bid position(s) for a price level
  override def toString: String = s"Bid(price=${CryptoValue.formatDecimal(price)}, amount=${CryptoValue.formatDecimal(quantity)})"
}

case class Ask(price: Double, quantity: Double) { // An ask is an offer to sell an asset; (likely aggregated) ask position(s) for a price level
  override def toString: String = s"Ask(price=${CryptoValue.formatDecimal(price)}, amount=${CryptoValue.formatDecimal(quantity)})"
}

case class OrderBook(exchange: String,
                     tradePair: TradePair,
                     bids: Map[Double, Bid], // price-level -> bid
                     asks: Map[Double, Ask], // price-level -> ask
                     lastUpdated: LocalDateTime) {

  def toCondensedString: String = {
    val bestBid = highestBid
    val bestAsk = lowestAsk
    s"${bids.keySet.size} Bids (highest price: ${CryptoValue.formatDecimal(bestBid.price)}, quantity: ${bestBid.quantity}) " +
      s"${asks.keySet.size} Asks(lowest price: ${CryptoValue.formatDecimal(bestAsk.price)}, quantity: ${bestAsk.quantity})"
  }

  def highestBid: Bid = bids(bids.keySet.max)

  def lowestAsk: Ask = asks(asks.keySet.min)
}

/**
 * Exchange-part of the global data structure the TPDataManager shall write to
 */
case class TPData(ticker: concurrent.Map[TradePair, Ticker],
                  extendedTicker: concurrent.Map[TradePair, ExtendedTicker],
                  orderBook: concurrent.Map[TradePair, OrderBook])

object TPDataManager {
  case class InitCheck()
  case class Initialized(tradePair: TradePair)
  case class StartStreamRequest(sink: Sink[Seq[TPStreamData], Future[Done]])

  def props(exchangeName: String, tradePair: TradePair, exchangeDataChannel: ActorRef, exchange: ActorRef,
            tpDataChannelInit: Function1[TPDataChannelPropsParams, Props], tpDataSink: TPData): Props =
    Props(new TPDataManager(exchangeName, tradePair, exchangeDataChannel, exchange, tpDataChannelInit, tpDataSink))
}

/**
 * Manages all kind of data of one tradepair at one exchange
 */
case class TPDataManager(exchangeName: String, tradePair: TradePair, exchangeDataChannel: ActorRef, exchange: ActorRef,
                         tpDataChannelInit: Function1[TPDataChannelPropsParams, Props],
                         tpData: TPData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TPDataManager])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private var tpDataChannel: ActorRef = _

  private var initializedMsgSend = false
  private var orderBookInitialized = false

  private var initTime: Instant = _

  val initCheckSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(1.seconds, 1.seconds, self, InitCheck())

  def initialized: Boolean = tpData.ticker.isDefinedAt(tradePair) && orderBookInitialized

  val sink: Sink[Seq[TPStreamData], Future[Done]] = Sink.foreach[Seq[TPStreamData]] { // TODO test foreachAsync
    data: Seq[TPStreamData] =>
      data.foreach {
        case t: Ticker =>
          tpData.ticker += tradePair -> t

        case t: ExtendedTicker =>
          tpData.extendedTicker += tradePair -> t

        case o: OrderBookSnapshot =>
          tpData.orderBook += tradePair ->
            OrderBook(
              exchangeName,
              tradePair,
              o.bids.filterNot(_.quantity == 0.0d).map(e => (e.price, e)).toMap,
              o.asks.filterNot(_.quantity == 0.0d).map(e => (e.price, e)).toMap,
              LocalDateTime.now
            )
          orderBookInitialized = true

        case o: OrderBookUpdate =>
          if (tpData.orderBook.isDefinedAt(tradePair)) {
            tpData.orderBook += tradePair ->
              OrderBook(
                exchangeName,
                tradePair,
                (tpData.orderBook(tradePair).bids ++ o.bids.map(e => e.price -> e)).filterNot(_._2.quantity == 0.0d),
                (tpData.orderBook(tradePair).asks ++ o.asks.map(e => e.price -> e)).filterNot(_._2.quantity == 0.0d),
                LocalDateTime.now
              )
          } else {
            log.warn(s"$o received, but no OrderBook present yet")
          }
      }
      eventuallyInitialized()
  }


  def eventuallyInitialized(): Unit = {
    if (!initializedMsgSend && initialized) {
      initializedMsgSend = true
      exchange ! Initialized(tradePair)
    }
  }

  override def preStart(): Unit = {
    initTime = Instant.now()
    tpDataChannel = context.actorOf(
      tpDataChannelInit.apply(TPDataChannelPropsParams(tradePair, exchangeDataChannel, self)), s"$exchangeName-TPDataChannel-$tradePair")

    tpDataChannel ! StartStreamRequest(sink)
  }

  def receive: Receive = {

    // Messages from Exchange
    case InitCheck() =>
      if (initialized) initCheckSchedule.cancel()
      else {
        if (Duration.between(initTime, Instant.now()).compareTo(AppConfig.dataManagerInitTimeout) > 0) {
          log.info(s"Killing $exchangeName-TPDataManager-$tradePair")
          self ! Kill // TODO graceful shutdown of tradepair-channel via parent actor
        }
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}