package org.purevalue.arbitrage.traderoom

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, PoisonPill, Props, Status}
import akka.stream.scaladsl.Sink
import org.purevalue.arbitrage.traderoom.ExchangePublicTPDataManager._
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

trait ExchangeTPStreamData
case class Heartbeat(ts:Instant) extends ExchangeTPStreamData
case class Ticker(exchange: String,
                  tradePair: TradePair,
                  highestBidPrice: Double,
                  highestBidQuantity: Option[Double],
                  lowestAskPrice: Double,
                  lowestAskQuantity: Option[Double],
                  lastPrice: Option[Double]) extends ExchangeTPStreamData {
  def priceEstimate: Double = lastPrice match {
    case Some(last) => (highestBidPrice + last + lowestAskPrice) / 3
    case None => (highestBidPrice + lowestAskPrice) / 2
  }
}
case class OrderBookSnapshot(bids: Seq[Bid], asks: Seq[Ask]) extends ExchangeTPStreamData
case class OrderBookUpdate(bids: Seq[Bid], asks: Seq[Ask]) extends ExchangeTPStreamData
case class Balances(all: List[Balance]) extends ExchangeTPStreamData

//////////////////////

/**
 * A bid is an offer to buy an asset; (likely aggregated) bid position(s) for a price level
 */
case class Bid(price: Double, quantity: Double) {
  override def toString: String = s"Bid(price=${formatDecimal(price)}, amount=${formatDecimal(quantity)})"
}

/**
 * An ask is an offer to sell an asset; (likely aggregated) ask position(s) for a price level
 */
case class Ask(price: Double, quantity: Double) {
  override def toString: String = s"Ask(price=${formatDecimal(price)}, amount=${formatDecimal(quantity)})"
}

case class OrderBook(exchange: String,
                     tradePair: TradePair,
                     bids: Map[Double, Bid], // price-level -> bid
                     asks: Map[Double, Ask]) { // price-level -> ask
  def toCondensedString: String = {
    val bestBid = highestBid
    val bestAsk = lowestAsk
    s"${bids.keySet.size} Bids (highest price: ${formatDecimal(bestBid.price)}, quantity: ${bestBid.quantity}) " +
      s"${asks.keySet.size} Asks(lowest price: ${formatDecimal(bestAsk.price)}, quantity: ${bestAsk.quantity})"
  }

  def highestBid: Bid = bids(bids.keySet.max)

  def lowestAsk: Ask = asks(asks.keySet.min)
}

case class TPDataTimestamps(var heartbeatTS: Option[Instant],
                            var tickerTS: Instant,
                            var orderBookTS: Option[Instant])
/**
 * Exchange-part of the global data structure the TPDataManager shall write to
 */
case class ExchangeTPData(ticker: concurrent.Map[TradePair, Ticker],
                          orderBook: concurrent.Map[TradePair, OrderBook],
                          age: TPDataTimestamps) {
  def readonly: ExchangeTPDataReadonly = ExchangeTPDataReadonly(ticker, orderBook)
}
case class ExchangeTPDataReadonly(ticker: collection.Map[TradePair, Ticker],
                                  orderBook: collection.Map[TradePair, OrderBook])

object ExchangePublicTPDataManager {
  case class InitCheck()
  case class Initialized(tradePair: TradePair)
  case class StartStreamRequest(sink: Sink[Seq[ExchangeTPStreamData], Future[Done]])
  case class Stop()

  def props(config: ExchangeConfig,
            tradePair: TradePair,
            exchangePublicDataInquirer: ActorRef,
            exchange: ActorRef,
            publicTPDataChannelProps: Function3[ExchangeConfig, TradePair, ActorRef, Props],
            tpDataSink: ExchangeTPData): Props =
    Props(new ExchangePublicTPDataManager(config, tradePair, exchangePublicDataInquirer, exchange, publicTPDataChannelProps, tpDataSink))
}

/**
 * Manages all kind of data of one tradepair at one exchange
 */
case class ExchangePublicTPDataManager(config: ExchangeConfig,
                                       tradePair: TradePair,
                                       exchangePublicDataInquirer: ActorRef,
                                       exchange: ActorRef,
                                       exchangePublicTPDataChannelProps: Function3[ExchangeConfig, TradePair, ActorRef, Props],
                                       tpData: ExchangeTPData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangePublicTPDataManager])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private var tpDataChannel: ActorRef = _

  private var initializedMsgSend = false
  private var orderBookInitialized = false

  private var initTimestamp: Instant = _
  private val stopData: AtomicBoolean = new AtomicBoolean(false)

  val initCheckSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(1.seconds, 1.seconds, self, InitCheck())

  def initialized: Boolean =
    tpData.ticker.isDefinedAt(tradePair) &&
      (if (config.orderBooksEnabled) orderBookInitialized else true)

  val sink: Sink[Seq[ExchangeTPStreamData], Future[Done]] = Sink.foreach[Seq[ExchangeTPStreamData]] {
    applyDataset
  }

  private def applyDataset(data: Seq[ExchangeTPStreamData]): Unit = {
    if (!stopData.get()) {
      data.foreach {
        case h: Heartbeat =>
          tpData.age.heartbeatTS = Some(h.ts)

        case t: Ticker =>
          tpData.ticker += tradePair -> t
          tpData.age.tickerTS = Instant.now

        case o: OrderBookSnapshot =>
          tpData.orderBook += tradePair ->
            OrderBook(
              config.exchangeName,
              tradePair,
              o.bids.filterNot(_.quantity == 0.0d).map(e => (e.price, e)).toMap,
              o.asks.filterNot(_.quantity == 0.0d).map(e => (e.price, e)).toMap
            )
          tpData.age.orderBookTS = Some(Instant.now)
          orderBookInitialized = true

        case o: OrderBookUpdate =>
          if (tpData.orderBook.isDefinedAt(tradePair)) {
            tpData.orderBook += tradePair ->
              OrderBook(
                config.exchangeName,
                tradePair,
                (tpData.orderBook(tradePair).bids ++ o.bids.map(e => e.price -> e)).filterNot(_._2.quantity == 0.0d),
                (tpData.orderBook(tradePair).asks ++ o.asks.map(e => e.price -> e)).filterNot(_._2.quantity == 0.0d)
              )
            tpData.age.orderBookTS = Some(Instant.now)
          } else {
            log.warn(s"$o received, but no OrderBook present yet")
          }

        case _ => throw new NotImplementedError
      }
      eventuallyInitialized()
    }
  }

  def eventuallyInitialized(): Unit = {
    if (!initializedMsgSend && initialized) {
      initializedMsgSend = true
      exchange ! Initialized(tradePair)
    }
  }

  override def preStart(): Unit = {
    initTimestamp = Instant.now()
    tpDataChannel = context.actorOf(
      exchangePublicTPDataChannelProps.apply(config, tradePair, exchangePublicDataInquirer), s"${config.exchangeName}-TPDataChannel-$tradePair")

    tpDataChannel ! StartStreamRequest(sink)
  }

  def receive: Receive = {

    // Messages from Exchange
    case InitCheck() =>
      if (initialized) initCheckSchedule.cancel()
      else {
        if (Duration.between(initTimestamp, Instant.now()).compareTo(Config.dataManagerInitTimeout) > 0) {
          log.info(s"Killing ${config.exchangeName}-TPDataManager-$tradePair")
          self ! Kill
        }
      }

    case Stop() =>
      stopData.set(true)
      self ! PoisonPill

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}