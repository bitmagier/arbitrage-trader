package org.purevalue.arbitrage.adapter

import java.time.{Duration, Instant}

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, Props, Status}
import org.purevalue.arbitrage.adapter.ExchangePublicDataManager._
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.traderoom.exchange.Exchange.ExchangePublicDataChannelInit
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, Main, TradeRoomConfig}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

trait ExchangePublicStreamData
case class Heartbeat(ts: Instant) extends ExchangePublicStreamData
case class Ticker(exchange: String,
                  tradePair: TradePair,
                  highestBidPrice: Double,
                  highestBidQuantity: Option[Double],
                  lowestAskPrice: Double,
                  lowestAskQuantity: Option[Double],
                  lastPrice: Option[Double]) extends ExchangePublicStreamData {
  def priceEstimate: Double = lastPrice match {
    case Some(last) => (highestBidPrice + last + lowestAskPrice) / 3
    case None => (highestBidPrice + lowestAskPrice) / 2
  }
}

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

case class PublicDataTimestamps(var heartbeatTS: Option[Instant],
                                var tickerTS: Instant)

case class ExchangePublicData(ticker: concurrent.Map[TradePair, Ticker],
                              age: PublicDataTimestamps) {
  def readonly: ExchangePublicDataReadonly = ExchangePublicDataReadonly(ticker)
}
case class ExchangePublicDataReadonly(ticker: collection.Map[TradePair, Ticker])

object ExchangePublicDataManager {
  case class InitTimeoutCheck()
  case class Initialized()
  case class IncomingData(data: Seq[ExchangePublicStreamData])

  def props(globalConfig:GlobalConfig,
            exchangeConfig: ExchangeConfig,
            tradeRoomConfig: TradeRoomConfig,
            tradePairs: Set[TradePair],
            exchangePublicDataInquirer: ActorRef,
            exchange: ActorRef,
            publicDataChannelProps: ExchangePublicDataChannelInit,
            publicData: ExchangePublicData): Props =
    Props(new ExchangePublicDataManager(globalConfig, exchangeConfig, tradeRoomConfig, tradePairs, exchangePublicDataInquirer, exchange, publicDataChannelProps, publicData))
}

/**
 * Manages all sorts of public data streams at one exchange
 */
case class ExchangePublicDataManager(globalConfig:GlobalConfig,
                                     exchangeConfig: ExchangeConfig,
                                     tradeRoomConfig: TradeRoomConfig,
                                     tradePairs: Set[TradePair],
                                     exchangePublicDataInquirer: ActorRef,
                                     exchange: ActorRef,
                                     exchangePublicDataChannelProps: ExchangePublicDataChannelInit,
                                     publicData: ExchangePublicData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangePublicDataManager])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private var publicDataChannel: ActorRef = _

  private val initStartDeadline: Instant = Instant.now.plus(tradeRoomConfig.dataManagerInitTimeout)
  private var tickerCompletelyInitialized: Boolean = false

  val initCheckSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(1.seconds, 1.seconds, self, InitTimeoutCheck())

  def onTickerUpdate(): Unit = {
    if (!tickerCompletelyInitialized) {
      tickerCompletelyInitialized = tradePairs.subsetOf(publicData.ticker.keySet)
      if (tickerCompletelyInitialized) {
        exchange ! Initialized()
      }
    }
  }

  private def applyDataset(data: Seq[ExchangePublicStreamData]): Unit = {
    data.foreach {
      case h: Heartbeat =>
        publicData.age.heartbeatTS = Some(h.ts)

      case t: Ticker =>
        publicData.ticker += t.tradePair -> t
        publicData.age.tickerTS = Instant.now
        onTickerUpdate()

      case other =>
        log.error(s"Not implemended: $other")
        throw new NotImplementedError
    }
  }

  def initTimeoutCheck(): Unit = {
    if (tickerCompletelyInitialized) initCheckSchedule.cancel()
    else {
      if (Instant.now.isAfter(initStartDeadline)) {
        log.info(s"Init timeout -> killing actor")
        self ! Kill
      }
    }
  }

  override def preStart(): Unit = {
    publicDataChannel = context.actorOf(exchangePublicDataChannelProps(globalConfig, exchangeConfig, self, exchangePublicDataInquirer),
    s"${exchangeConfig.exchangeName}-PublicDataChannel")
  }


  override def receive: Receive = {
    // @formatter:off
    case IncomingData(data)    => applyDataset(data)
    case InitTimeoutCheck()    => initTimeoutCheck()
    case Status.Failure(cause) => log.error("received failure", cause)
    // @formatter:on
  }
}