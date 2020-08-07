package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange._
import org.purevalue.arbitrage.TPDataManager.{Ask, Bid}
import org.purevalue.arbitrage.adapter.ExchangeDataChannel.{GetTradePairs, TradePairs}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

case class Ticker(exchange: String,
                  tradePair: TradePair,
                  highestBidPrice: Double,
                  highestBidQuantity: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  lowestAskPrice: Double,
                  lowestAskQuantity: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  lastPrice: Double,
                  lastQuantity: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  weightedAveragePrice: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  lastUpdated: LocalDateTime)

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

case class Wallet(exchange: String,
                  assets: Map[Asset, Double])

case class Fee(exchange: String,
               makerFee: Double,
               takerFee: Double)


object Exchange {
  case class IsInitialized()
  case class IsInitializedResponse(initialized: Boolean)
  case class GetWallet()
  case class GetTickers()
  case class GetOrderBooks()
  case class GetFee()

  def props(name: String, config: ExchangeConfig, exchangeDataChannel: ActorRef, tpDataChannelPropsInit: Function1[TPDataChannelPropsParams, Props]): Props =
    Props(new Exchange(name, config, exchangeDataChannel, tpDataChannelPropsInit))
}

case class Exchange(exchangeName: String, config: ExchangeConfig, exchangeDataChannel: ActorRef, tpDataChannelPropsInit:Function1[TPDataChannelPropsParams, Props]) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val fee: Fee = Fee(exchangeName, config.makerFee, config.takerFee)
  // dynamic
  var tradePairs: Set[TradePair] = _
  var tpDataManagers: Map[TradePair, ActorRef] = Map()
  var tpDataInitPending: Set[TradePair] = _

  def initialized: Boolean = tpDataInitPending != null && tpDataInitPending.isEmpty

  var wallet: Wallet = Wallet(exchangeName, // TODO ask exchange adapter for that
    Map(
      Asset("BTC") -> 0.5d,
      Asset("USDT") -> 2000.0d,
      Asset("ETH") -> 10.0,
      Asset("ADA") -> 100000.0,
      Asset("ERD") -> 100000.0d,
      Asset("ALGO") -> 50000.0d,
    )
  )

  def initTradePairBasedData(): Unit = {
    tpDataInitPending = tradePairs
    for (p <- tradePairs) {
      tpDataManagers = tpDataManagers + (p -> context.actorOf(TPDataManager.props(exchangeName, p, exchangeDataChannel, self,
        tpDataChannelPropsInit),
        s"$exchangeName.TPDataManager-${p.baseAsset.officialSymbol}-${p.quoteAsset.officialSymbol}"))
    }
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 15.minutes, loggingEnabled = true) {
      case _ => Restart
    }


  override def preStart(): Unit = {
    log.info(s"Initializing exchange $exchangeName")
    exchangeDataChannel ! GetTradePairs
  }

  override def receive: Receive = {

    // Messages from TradeRoom
    case IsInitialized() =>
      val result: Boolean = initialized
      if (!result) {
        log.debug(s"[$exchangeName] initialization pending: $tpDataInitPending")
      }
      sender() ! Exchange.IsInitializedResponse(initialized)

    case GetWallet() =>
      if (initialized) {
        sender() ! wallet
      } else {
        log.debug(s"[$exchangeName] We have been asked to deliver the Wallet, but we are not yet fully initialized")
      }

    case GetOrderBooks() =>
      if (initialized) {
        implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
        var orderBooks = List[Future[OrderBook]]()
        for (m <- tpDataManagers.values) {
          orderBooks = (m ? TPDataManager.GetOrderBook()).mapTo[OrderBook] :: orderBooks
        }
        Future.sequence(orderBooks).pipeTo(sender())
      } else {
        log.debug(s"[$exchangeName] We have been asked to deliver OrderBooks, but we are not yet fully initialized")
      }

    case GetTickers() =>
      if (initialized) {
        implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
        var ticker = List[Future[Ticker]]()
        for (m <- tpDataManagers.values) {
          ticker = (m ? TPDataManager.GetTicker()).mapTo[Ticker] :: ticker
        }
        Future.sequence(ticker).pipeTo(sender())
      } else {
        log.debug(s"[$exchangeName] We have been asked to deliver Ticker, but we are not yet fully initialized")
      }

    case GetFee() =>
      if (initialized) {
        sender() ! fee
      } else {
        log.debug(s"[$exchangeName] We have been asked to deliver Fee, but we are not yet fully initialized")
      }

    // Messages from ExchangeAdapter

    case TradePairs(t) =>
      tradePairs = t
      log.info(s"$exchangeName: ${tradePairs.size} TradePairs: $tradePairs")
      initTradePairBasedData()

    // Messages from TradePairDataManager

    case TPDataManager.Initialized(t) =>
      tpDataInitPending -= t
      log.info(s"[$exchangeName]: [$t] initialized. Still pending: $tpDataInitPending")
      if (tpDataInitPending.isEmpty) {
        log.info(s"${Emoji.Robot} [$exchangeName]: all TradePair data initialized and running")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO later: query fee dynamically