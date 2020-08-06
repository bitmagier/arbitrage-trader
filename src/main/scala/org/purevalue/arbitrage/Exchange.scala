package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange._
import org.purevalue.arbitrage.TradePairDataManager.{AskPosition, BidPosition}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.{GetTradePairs, TradePairs}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future}

case class Ticker(exchange:String,
                  tradePair: TradePair,
                  highestBidPrice: Double,
                  highestBidQuantity: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  lowestAskPrice: Double,
                  lowestAskQuantity: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  lastPrice: Double,
                  lastQuantity: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  weightedAveragePrice: Option[Double], // is not available at some exchanges (e.g. bitfinex)
                  lastUpdated: LocalDateTime)

case class OrderBook(exchange:String,
                     tradePair: TradePair,
                     bids: Map[Double, BidPosition], // price-level -> bid
                     asks: Map[Double, AskPosition], // price-level -> ask
                     lastUpdated: LocalDateTime) {

  def toCondensedString: String = {
    val bestBid = highestBid
    val bestAsk = lowestAsk
    s"${bids.keySet.size} Bids (highest price: ${CryptoValue.formatPrice(bestBid.price)}, quantity: ${bestBid.qantity}) " +
      s"${asks.keySet.size} Asks(lowest price: ${CryptoValue.formatPrice(bestAsk.price)}, quantity: ${bestAsk.qantity})"
  }

  def highestBid:BidPosition = bids(bids.keySet.max)
  def lowestAsk:AskPosition = asks(asks.keySet.min)
}

case class Wallet(exchange: String,
                  assets: Map[Asset, Double])

case class Fee(exchange: String,
               makerFee: Double,
               takerFee: Double)


object Exchange {
  case class IsInitialized()
  case class IsInitializedResponse(initialized:Boolean)
  case class GetWallet()
  case class GetTickers()
  case class GetOrderBooks()
  case class GetFee()

  def props(name: String, config: ExchangeConfig, adapter: ActorRef): Props = Props(new Exchange(name, config, adapter))
}

case class Exchange(name: String, config: ExchangeConfig, exchangeAdapter: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val assets: Set[Asset] = GlobalConfig.AllAssets.filter(e => config.assets.contains(e._2.officialSymbol)).values.toSet
  val fee: Fee = Fee(name, config.makerFee, config.takerFee)
  // dynamic
  var tradePairs: Set[TradePair] = _
  var tradePairDataManagers: Map[TradePair, ActorRef] = Map()
  var tradePairDataInitPending: Set[TradePair] = _

  def initialized: Boolean = tradePairDataInitPending != null && tradePairDataInitPending.isEmpty

  var wallet: Wallet = Wallet(name, // TODO ask exchange adapter for that
    Map(
      Asset("BTC") -> 0.5d,
      Asset("USDT") -> 2000.0d,
      Asset("ETH") -> 10.0,
      Asset("ADA") -> 0.0,
      Asset("ERD") -> 100000.0d,
      Asset("ALGO") -> 50000.0d,
      Asset("BTG") -> 500.0d)
  )

  def initTradePairBasedData(): Unit = {
    tradePairDataInitPending = tradePairs
    for (p <- tradePairs) {
      tradePairDataManagers = tradePairDataManagers + (p -> context.actorOf(TradePairDataManager.props(name, p, exchangeAdapter, self),
        s"$name.TradePairDataManager-${p.baseAsset.officialSymbol}-${p.quoteAsset.officialSymbol}"))
    }
  }

  override def preStart(): Unit = {
    log.info(s"Initializing exchange $name")
    exchangeAdapter ! GetTradePairs
  }

  override def receive: Receive = {

    // Messages from TradeRoom
    case IsInitialized() =>
      val result:Boolean = initialized
      if (!result) {
        log.debug(s"[$name] initialization pending: $tradePairDataInitPending")
      }
      sender() ! Exchange.IsInitializedResponse(initialized)

    case GetWallet() =>
      if (initialized) {
        sender() ! wallet
      } else {
        log.debug(s"[$name] We have been asked to deliver the Wallet, but we are not yet fully initialized")
      }

    case GetOrderBooks() =>
      if (initialized) {
        implicit val timeout: Timeout = StaticConfig.tradeRoom.internalCommunicationTimeout
        var orderBooks = List[Future[OrderBook]]()
        for (m <- tradePairDataManagers.values) {
          orderBooks = (m ? TradePairDataManager.GetOrderBook()).mapTo[OrderBook] :: orderBooks
        }
        Future.sequence(orderBooks).pipeTo(sender())
      } else {
        log.debug(s"[$name] We have been asked to deliver OrderBooks, but we are not yet fully initialized")
      }

    case GetTickers() =>
      if (initialized) {
        implicit val timeout: Timeout = StaticConfig.tradeRoom.internalCommunicationTimeout
        var ticker = List[Future[Ticker]]()
        for (m <- tradePairDataManagers.values) {
          ticker = (m ? TradePairDataManager.GetTicker()).mapTo[Ticker] :: ticker
        }
        Future.sequence(ticker).pipeTo(sender())
      } else {
        log.debug(s"[$name] We have been asked to deliver Ticker, but we are not yet fully initialized")
      }

    case GetFee() =>
      if (initialized) {
        sender() ! fee
      } else {
        log.debug(s"[$name] We have been asked to deliver Fee, but we are not yet fully initialized")
      }

    // Messages from ExchangeAdapter

    case TradePairs(t) =>
      tradePairs = t
      log.info(s"$name: ${tradePairs.size} TradePairs received: $tradePairs")
      initTradePairBasedData()

    // Messages from TradePairDataManager

    case TradePairDataManager.Initialized(t) =>
      tradePairDataInitPending -= t
      log.info(s"[$name]: [$t] initialized. Still pending: $tradePairDataInitPending")
      if (tradePairDataInitPending.isEmpty) {
        log.info(s"${Emoji.Robot} [$name]: all TradePair data initialized and running")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO later: query fee dynamically