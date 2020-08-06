package org.purevalue.arbitrage

import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.{GetFee, GetOrderBooks, GetTickers, GetWallet}
import org.purevalue.arbitrage.TradePairDataManager.{AskPosition, BidPosition, GetOrderBook}
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
  var tradePairDataManagers: Map[TradePair, ActorRef] = _
  var tradePairDataInitPending: Set[TradePair] = _

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
    tradePairDataManagers = Map[TradePair, ActorRef]()
    for (p <- tradePairs) {
      tradePairDataManagers += (p -> context.actorOf(TradePairDataManager.props(name, p, exchangeAdapter, self),
        s"$name.TradePairManager-${p.baseAsset.officialSymbol}-${p.quoteAsset.officialSymbol}"))
    }
  }

  override def preStart(): Unit = {
    log.info(s"Initializing exchange $name")
    exchangeAdapter ! GetTradePairs
  }

  override def receive: Receive = {

    // Messages from TradeRoom

    case GetWallet() =>
      sender() ! wallet

    case GetOrderBooks() =>
      implicit val timeout: Timeout = StaticConfig.tradeRoom.internalCommunicationTimeout
      var orderBooks = List[Future[OrderBook]]()
      for (m <- tradePairDataManagers.values) {
        orderBooks = (m ? GetOrderBook()).mapTo[OrderBook] :: orderBooks
      }
      Future.sequence(orderBooks).pipeTo(sender())

    case GetTickers() =>
      implicit val timeout: Timeout = StaticConfig.tradeRoom.internalCommunicationTimeout
      var ticker = List[Future[Ticker]]()
      for (m <- tradePairDataManagers.values) {
        ticker = (m ? GetTickers()).mapTo[Ticker] :: ticker
      }
      Future.sequence(ticker).pipeTo(sender())

    case GetFee() =>
      sender() ! fee

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