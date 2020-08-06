package org.purevalue.arbitrage

import java.time.{Duration, Instant, LocalDateTime, ZonedDateTime}
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.{GetFee, GetOrderBooks, GetTickers, GetWallet}
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexAdapter
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

sealed trait TradeDirection
object TradeDirection extends TradeDirection {
  case object Buy extends TradeDirection
  case object Sell extends TradeDirection
}

case class CryptoValue(asset: Asset, amount: Double) {

  def convertTo(asset: Asset, dc: TradeDecisionContext): Double = {
    if (this.asset == asset) amount
    else {
      amount * dc.referenceTicker(TradePair.of(this.asset, asset)).lastPrice
    }
  }
}

/** Order: a single trade request before it's execution */
case class Order(id: UUID,
                 orderBundleId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 direction: TradeDirection,
                 fee: Fee,
                 amountBaseAsset: Option[Double], // is filled when buy baseAsset
                 amountQuoteAsset: Option[Double], // is filled when sell baseAsset
                 limit: Double) {
  var placed: Boolean = false
  var placementTime: ZonedDateTime = _

  def setPlaced(ts: ZonedDateTime): Unit = {
    placed = true
    placementTime = ts
  }

  // costs & gains. positive value means we have a win, negative value means a loss
  def bill: Seq[CryptoValue] = {
    if (direction == TradeDirection.Buy)
      Seq(
        CryptoValue(tradePair.baseAsset, amountBaseAsset.get),
        CryptoValue(tradePair.quoteAsset, -limit),
        CryptoValue(tradePair.quoteAsset, -limit * fee.takerFee) // for now we just take the higher taker fee
      )
    else
      Seq(
        CryptoValue(tradePair.baseAsset, -limit),
        CryptoValue(tradePair.baseAsset, -limit * fee.takerFee),
        CryptoValue(tradePair.quoteAsset, amountQuoteAsset.get),
      )
  }
}

/** Trade: a successfully executed Order */
case class Trade(id: UUID,
                 orderId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 direction: TradeDirection,
                 fee: Fee,
                 amountBaseAsset: Double,
                 amountQuoteAsset: Double,
                 executionRate: Double,
                 executionTime: ZonedDateTime,
                 wasMaker: Boolean,
                 supervisorComments: Seq[String]) // comments from TradeRoom "operator"

case class CompletedOrderBundle(orderBundle: OrderBundle,
                                executedAsInstructed: Boolean,
                                executedTrades: Seq[Trade],
                                cancelledOrders: Seq[Order],
                                bill: Seq[CryptoValue],
                                earningUSDT: Double)

object TradeRoom {
  // communication with trader INCOMING
  case class GetTradeDecisionContext()
  /** High level order bundle, covering 2 or more trader orders */
  case class OrderBundle(id: UUID,
                         traderName: String,
                         trader: ActorRef,
                         creationTime: LocalDateTime,
                         orders: Seq[Order],
                         estimatedWin: CryptoValue, // TODO converted to USDT
                         decisionComment: String)
  // communication with trader OUTGOING
  case class TradeDecisionContext(tickers: Map[TradePair, Map[String, Ticker]],
                                  orderBooks: Map[TradePair, Map[String, OrderBook]],
                                  walletPerExchange: Map[String, Wallet],
                                  feePerExchange: Map[String, Fee]) {
    def referenceTicker(tradePair: TradePair): Ticker = {
      var exchanges = StaticConfig.tradeRoom.referenceTickerExchanges
      var ticker: Ticker = null
      while (ticker == null) { // fallback implementation if primary ticker is not here because of exchange down or tiucker update too old
        tickers(tradePair).get(exchanges.head) match {
          case Some(t) => ticker = t
          case None => exchanges = exchanges.tail
        }
      }
      ticker
    }
  }

  case class OrderBundlePlaced(orderBundleId: UUID)
  case class OrderBundleCompleted(orderBundle: CompletedOrderBundle)

  // communication with exchange INCOMING
  case class OrderPlaced(orderId: UUID, placementTime: ZonedDateTime)
  case class OrderExecuted(orderId: UUID, trade: Trade)
  case class OrderCancelled(orderId: UUID)
  // communication with exchange OUTGOING
  case class PlaceOrder(order: Order)
  case class CancelOrder()

  def props(config: TradeRoomConfig): Props = Props(new TradeRoom(config))
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom(config: TradeRoomConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  var exchanges: Map[String, ActorRef] = Map()
  var traders: Map[String, ActorRef] = Map()
  var dcCache: Option[TradeDecisionContext] = None
  var dcCacheTimestamp: Instant = _

  var activeOrders: Map[UUID, Order] = Map() // orderId -> Order; orders, belonging to activeOrderBundles & active at the corresponding exchange
  var tradesPerActiveOrderBundle: Map[UUID, ListBuffer[Trade]] = Map() // orderBundleId -> Trade; trades, belonging to activeOrderBundles & executed at an exchange
  var activeOrderBundles: Map[UUID, OrderBundle] = Map()

  // TODO buffer+persist completed order bundles to a database instead (cassandra?)
  var completedOrderBundles: Map[UUID, CompletedOrderBundle] = Map() // orderBundleID -> CompletedOrderBundle

  override def preStart(): Unit = {
    exchanges += "binance" -> context.actorOf(Exchange.props("binance", StaticConfig.exchange("binance"),
      context.actorOf(BinanceAdapter.props(StaticConfig.exchange("binance")), "BinanceAdapter")), "binance")

    exchanges += "bitfinex" -> context.actorOf(Exchange.props("bitfinex", StaticConfig.exchange("bitfinex"),
      context.actorOf(BitfinexAdapter.props(StaticConfig.exchange("bitfinex")), "BitfinexAdapter")), "bitfinex")

    log.info(s"Initializing exchanges: ${exchanges.keys}")

    //    traders += "FooTrader" -> context.actorOf(FooTrader.props(StaticConfig.trader("foo-trader")), "FooTrader")
  }

  def calculateBill(trades: List[Trade]): Seq[CryptoValue] = { // TODO move calc function to Trade class
    val rawInvoice = ArrayBuffer[(Asset, Double)]()
    for (trade <- trades) {
      val feeRate = if (trade.wasMaker) trade.fee.makerFee else trade.fee.takerFee
      if (trade.direction == TradeDirection.Buy) {
        rawInvoice += ((trade.tradePair.baseAsset, trade.amountBaseAsset.abs))
        rawInvoice += ((trade.tradePair.quoteAsset, -trade.amountQuoteAsset.abs))
        rawInvoice += ((trade.tradePair.quoteAsset, -(trade.amountQuoteAsset.abs * feeRate)))
      } else if (trade.direction == TradeDirection.Sell) {
        rawInvoice += ((trade.tradePair.baseAsset, -trade.amountBaseAsset.abs))
        rawInvoice += ((trade.tradePair.baseAsset, -(trade.amountBaseAsset.abs * feeRate)))
        rawInvoice += ((trade.tradePair.quoteAsset, trade.amountQuoteAsset.abs))
      }
    }

    var invoiceAggregated = Map[Asset, Double]()
    rawInvoice.foreach { e =>
      invoiceAggregated += (e._1 -> (e._2 + invoiceAggregated.getOrElse(e._1, 0.0d)))
    }
    invoiceAggregated.map(e => CryptoValue(e._1, e._2)).toSeq
  }

  def isUpToDate(ob: OrderBook, now: LocalDateTime): Boolean = {
    Duration.between(ob.lastUpdated, now).compareTo(config.maxDataAge) <= 0
  }

  def isUpToDate(t: Ticker, now: LocalDateTime): Boolean = {
    Duration.between(t.lastUpdated, now).compareTo(config.maxDataAge) <= 0
  }

  def collectTradableAssets(): Future[TradeDecisionContext] = {
    implicit val timeout: Timeout = Timeout(2.seconds) // TODO configuration
    var tickerFutures = List[Future[List[Ticker]]]()
    var orderBookFutures = List[Future[List[OrderBook]]]()
    var walletFutures = List[Future[Wallet]]()
    var feeFutures = List[Future[Fee]]()
    for (exchange <- exchanges.values) {
      tickerFutures = (exchange ? GetTickers()).mapTo[List[Ticker]] :: tickerFutures
      orderBookFutures = (exchange ? GetOrderBooks()).mapTo[List[OrderBook]] :: orderBookFutures
      walletFutures = (exchange ? GetWallet()).mapTo[Wallet] :: walletFutures
      feeFutures = (exchange ? GetFee()).mapTo[Fee] :: feeFutures
    }
    val now = LocalDateTime.now()

    val t1: Future[List[Ticker]] = Future.sequence(tickerFutures).map(_.flatten)
    val futureTickers: Future[Map[TradePair, Map[String, Ticker]]] = t1.map { tickers =>
      var result = Map[TradePair, Map[String, Ticker]]()
      for (ticker <- tickers) {
        if (isUpToDate(ticker, now)) {
          result = result + (ticker.tradePair ->
            (result.getOrElse(ticker.tradePair, Map()) + (ticker.exchange -> ticker)))
        }
      }
      result
    }

    val o1: Future[List[OrderBook]] = Future.sequence(orderBookFutures).map(_.flatten)
    val futureOrderBooks: Future[Map[TradePair, Map[String, OrderBook]]] = o1.map { books =>
      var result = Map[TradePair, Map[String, OrderBook]]()
      for (book <- books) {
        if (isUpToDate(book, now)) {
          result = result + (book.tradePair ->
            (result.getOrElse(book.tradePair, Map()) + (book.exchange -> book)))
        }
      }
      result
    }
    val futureWallets: Future[Map[String, Wallet]] =
      Future.sequence(walletFutures)
        .map(e => e.map(w => (w.exchange, w)).toMap)

    val futureFees: Future[Map[String, Fee]] =
      Future.sequence(feeFutures)
        .map(e => e.map(f => (f.exchange, f)).toMap)

    for {
      orderBooks <- futureOrderBooks
      wallets <- futureWallets
      fees <- futureFees
      tickers <- futureTickers
    } yield TradeDecisionContext(tickers, orderBooks, wallets, fees)
  }

  def placeOrderBundleOrders(t: OrderBundle): Unit = {
    tradesPerActiveOrderBundle += (t.id -> ListBuffer())
    for (order <- t.orders) {
      exchanges(order.exchange) ! PlaceOrder(order)
    }
  }

  def orderPlaced(orderId: UUID, placementTime: ZonedDateTime): Option[OrderBundlePlaced] = {
    val order = activeOrders(orderId)
    order.setPlaced(placementTime)
    if (log.isTraceEnabled) log.trace(s"Order placed: $order ")

    if (activeOrderBundles(order.orderBundleId).orders.forall(_.placed)) {
      Some(OrderBundlePlaced(order.orderBundleId))
    } else {
      None
    }
  }

  def calculateEarningUSDT(bill: Seq[CryptoValue], dc: TradeDecisionContext): Double = {
    bill.map(_.convertTo(Asset("USDT"), dc)).sum
  }

  def orderExecuted(orderId: UUID, trade: Trade): Option[(ActorRef, OrderBundleCompleted)] = {
    var answer: Option[(ActorRef, OrderBundleCompleted)] = None
    val order = activeOrders(orderId)
    activeOrders -= orderId
    val orderBundleId = order.orderBundleId
    tradesPerActiveOrderBundle(orderBundleId) += trade

    val orderIds: Seq[UUID] = activeOrderBundles(orderBundleId).orders.map(_.id)
    if (orderIds.forall(orderId => !activeOrders.contains(orderId))) {
      // collect trades and complete OrderBundle
      val trades = tradesPerActiveOrderBundle(orderBundleId).toList
      val bill: Seq[CryptoValue] = calculateBill(trades)

      val completedOrderBundle = CompletedOrderBundle(
        activeOrderBundles(orderBundleId),
        executedAsInstructed = true,
        trades,
        Seq(),
        bill,
        calculateEarningUSDT(bill, dcCache.get)
      )

      completedOrderBundles += (orderBundleId -> completedOrderBundle)
      tradesPerActiveOrderBundle -= orderBundleId
      answer = Some((activeOrderBundles(orderBundleId).trader, OrderBundleCompleted(completedOrderBundle)))
      activeOrderBundles -= orderBundleId
    }
    answer
  }

  def checkIfEnoughBalanceAvailable(t: OrderBundle): Boolean = true // TODO seriously

  def receive: Receive = {

    // messages from Trader

    case GetTradeDecisionContext() =>
      if (dcCache.isDefined && Duration.between(dcCacheTimestamp, Instant.now()).toMillis > 500) { // TODO configure max cache age
        collectTradableAssets() onComplete {
          case Success(dc) => dcCache = Some(dc); dcCacheTimestamp = Instant.now()
          case Failure(exxeption) => log.error("collectTradableAssets failed", exxeption)
        }
      }
      sender() ! dcCache.get

    case t: OrderBundle =>
      if (checkIfEnoughBalanceAvailable(t)) { // this is the safety-check, the original check should have been done by trader already before
        placeOrderBundleOrders(t)
      }

    // messages from Exchange

    case OrderPlaced(orderId, placementTime) =>
      orderPlaced(orderId, placementTime) match {
        case Some(orderBundlePlaced) => sender() ! orderBundlePlaced
        case None =>
      }

    case OrderExecuted(orderId, trade) =>
      orderExecuted(orderId, trade) match {
        case Some((trader, responseToTrader)) => trader ! responseToTrader
        case None =>
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO shudown app in case of exception from any actor
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
// TODO check order books of opposide trade direction - assure we have exactly one order book per exchange and 2 assets