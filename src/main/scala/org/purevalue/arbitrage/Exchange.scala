package org.purevalue.arbitrage

import java.text.DecimalFormat
import java.time.LocalDateTime

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.{GetFee, GetOrderBooks, GetTickers, GetWallet}
import org.purevalue.arbitrage.TradePairDataManager.{AskPosition, BidPosition, GetOrderBook}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.{GetTradePairs, TradePairs}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

case class Ticker(exchange:String,
                  tradePair: TradePair,
                  bestBidPrice: Double,
                  bestBidQuantity: Double,
                  bestAskPrice: Double,
                  bestAskQuantity: Double,
                  lastPrice: Double,
                  lastQuantity: Double,
                  weightedAveragePrice: Double,
                  lastUpdated: LocalDateTime)

case class OrderBook(exchange:String,
                     tradePair: TradePair,
                     bids: Map[Double, BidPosition], // price-level -> bid
                     asks: Map[Double, AskPosition], // price-level -> ask
                     lastUpdated: LocalDateTime) {
  private def formatPrice(d: Double) = new DecimalFormat("#.##########").format(d)

  def toCondensedString: String =
    s"${bids.keySet.size} Bids(max price: ${formatPrice(bids.maxBy(_._2.price)._2.price)}) " +
      s"${asks.keySet.size} Asks(min price: ${formatPrice(asks.minBy(_._2.price)._2.price)})"

  def lowestBid:BidPosition = bids(bids.keySet.min)
  def highestAsk:AskPosition = asks(asks.keySet.max)
}

case class Wallet(exchange: String,
                  values: Map[Asset, Double])

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

  val assets: Set[Asset] = GlobalConfig.assets.filter(e => config.assets.contains(e._2.officialSymbol)).values.toSet
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
      tradePairDataManagers += (p -> context.actorOf(TradePairDataManager.props(name, p, exchangeAdapter, self), s"$name.OrderBook-${p.baseAsset.officialSymbol}-${p.quoteAsset.officialSymbol}"))
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
      implicit val timeout: Timeout = Timeout(2.seconds) // configuration
      var orderBooks = List[Future[OrderBook]]()
      for (m <- tradePairDataManagers.values) {
        orderBooks = (m ? GetOrderBook()).mapTo[OrderBook] :: orderBooks
      }
      Future.sequence(orderBooks).pipeTo(sender())

    case GetTickers() =>
      implicit val timeout: Timeout = Timeout(2.seconds)
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

    // Messages from OrderBookManager

    case TradePairDataManager.Initialized(t) =>
      tradePairDataInitPending -= t
      log.info(s"$name: OrderBook $t initialized. Still pending: $tradePairDataInitPending")
      if (tradePairDataInitPending.isEmpty) {
        log.info(s"$name: all OrderBooks initialized and running")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO later: query fee dynamically