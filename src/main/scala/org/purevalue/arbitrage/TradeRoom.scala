package org.purevalue.arbitrage

import java.text.DecimalFormat
import java.time.{Duration, Instant, LocalDateTime, ZonedDateTime}
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.CryptoValue.formatDecimal
import org.purevalue.arbitrage.Exchange._
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage.trader.FooTrader
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

sealed trait TradeDirection
object TradeDirection extends TradeDirection {
  case object Buy extends TradeDirection
  case object Sell extends TradeDirection
}

case class CryptoValue(asset: Asset, amount: Double) {

  def convertTo(targetAsset: Asset, dc: TradeDecisionContext): Option[Double] = {
    if (this.asset == targetAsset) Some(amount)
    else {
      // try direct conversion first
      dc.referenceTicker(TradePair.of(this.asset, targetAsset)) match {
        case Some(ticker) =>
            Some(amount * ticker.weightedAveragePrice.getOrElse(ticker.lastPrice))
        case None => // convert to BTC first and then to targetAsset
          val toBtcTicker = dc.referenceTicker(TradePair.of(this.asset, Asset("BTC")))
          if (toBtcTicker.isEmpty) return None

          val toBTCRate = toBtcTicker.get.weightedAveragePrice.getOrElse(toBtcTicker.get.lastPrice)
          val btcToTargetTicker = dc.referenceTicker(TradePair.of(Asset("BTC"), targetAsset))
          if (btcToTargetTicker.isEmpty) return None
          val btcToTargetRate = btcToTargetTicker.get.weightedAveragePrice.getOrElse(btcToTargetTicker.get.lastPrice)
          Some(amount * toBTCRate * btcToTargetRate)
      }
    }
  }

  override def toString: String = s"${formatDecimal(amount)} ${asset.officialSymbol}"
}
object CryptoValue {
  def formatDecimal(d: Double): String = new DecimalFormat("#.##########").format(d)
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
                                earningUSDT: Option[Double])

object TradeRoom {
  // communication with trader INCOMING
  case class GetTradeDecisionContext()
  /** High level order bundle, covering 2 or more trader orders */
  case class OrderBundle(id: UUID,
                         traderName: String,
                         trader: ActorRef,
                         creationTime: LocalDateTime,
                         orders: Seq[Order],
                         estimatedWinUSDT: Double,
                         decisionComment: String)
  // communication with trader OUTGOING
  case class TradeDecisionContext(tickers: Map[TradePair, Map[String, Ticker]],
                                  orderBooks: Map[TradePair, Map[String, OrderBook]],
                                  walletPerExchange: Map[String, Wallet],
                                  feePerExchange: Map[String, Fee]) {
    def referenceTicker(tradePair: TradePair): Option[Ticker] = {
      var exchanges = AppConfig.tradeRoom.referenceTickerExchanges
      var ticker: Ticker = null
      while (ticker == null) { // fallback implementation if primary ticker is not here because of exchange down or ticker update too old
        tickers.get(tradePair) match {
          case Some(map) => map.get(exchanges.head) match {
            case Some(t) => ticker = t
            case None => exchanges = exchanges.tail
          }
          case None => return None // no ticker for that tradepair available
        }
      }
      Some(ticker)
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

  def collectTradeDecisionContext(): Future[TradeDecisionContext] = {
    implicit val timeout: Timeout = config.internalCommunicationTimeout
    var tickerFutures = List[Future[List[Ticker]]]()
    var orderBookFutures = List[Future[List[OrderBook]]]()
    var walletFutures = List[Future[Wallet]]()
    var feeFutures = List[Future[Fee]]()
    for (exchange <- exchanges.values) {
      if (Await.result((exchange ? IsInitialized()).mapTo[IsInitializedResponse], timeout.duration).initialized) {
        tickerFutures = (exchange ? GetTickers()).mapTo[List[Ticker]] :: tickerFutures
        orderBookFutures = (exchange ? GetOrderBooks()).mapTo[List[OrderBook]] :: orderBookFutures
        walletFutures = (exchange ? GetWallet()).mapTo[Wallet] :: walletFutures
        feeFutures = (exchange ? GetFee()).mapTo[Fee] :: feeFutures
      }
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
    log.debug(s"got order bundle to place: $t")
    //    tradesPerActiveOrderBundle += (t.id -> ListBuffer())
    //    for (order <- t.orders) {
    //      exchanges(order.exchange) ! PlaceOrder(order)
    //    }
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

  def calculateEarningUSDT(bill: Seq[CryptoValue], dc: TradeDecisionContext): Option[Double] = {
    val converted = bill.map(
      _.convertTo(Asset("USDT"), dc)
    )
    if (converted.forall(_.isDefined)) {
      Some(converted.map(_.get).sum)
    } else {
      None
    }
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

  def checkIfEnoughBalanceAvailable(o: Order): Boolean = {
    // a possibly (somewhat) aged DC is good enough here
    val wallet: Wallet = dcCache.get.walletPerExchange(o.exchange)
    val coinsToSpend: CryptoValue = if (o.direction == TradeDirection.Buy) {
      CryptoValue(o.tradePair.quoteAsset, o.limit * (1.0d + o.fee.takerFee))
    } else {
      CryptoValue(o.tradePair.baseAsset, o.limit * (1.0d + o.fee.takerFee))
    }
    if (wallet.assets(coinsToSpend.asset) < coinsToSpend.amount) {
      log.warn(s" Wallet balance for ${coinsToSpend.asset} on exchange ${wallet.exchange} is not sufficient to spend $coinsToSpend")
      false
    } else {
      true
    }
  }

  def pullFreshTradeDecisionContext(): Option[TradeDecisionContext] = {
    if (dcCache.isEmpty ||
      Duration.between(dcCacheTimestamp, Instant.now()).toMillis > config.cachedDataLifetime.toMillis) {

      collectTradeDecisionContext() onComplete {
        case Success(dc) =>
          dcCache = Some(dc)
          dcCacheTimestamp = Instant.now()
        case Failure(e) =>
          log.warn(s"${Emoji.SadFace} collectTradableAssets() failed", e)
      }
    }
    if (dcCache.isEmpty) {
      None
    } else {
      val dataAge = Duration.between(dcCacheTimestamp, Instant.now())
      if (dataAge.toMillis > config.maxDataAge.toMillis) {
        log.error(s"${Emoji.SadAndConfused} cannot deliver TradeDecisionContext because our snapshot is out-dated (age: $dataAge)")
        None
      } else {
        dcCache
      }
    }
  }

  def checkBalanceSufficient(ob: OrderBundle): Boolean = {
    if (!ob.orders.forall(o => checkIfEnoughBalanceAvailable(o))) {
      log.warn(s"${Emoji.LookingDown} Cannot place order bundle because available balances are not sufficient")
      false
    } else true
  }


  def initExchange(name: String, exchangeInit:ExchangeInitStuff): Unit = {
    val camelName = name.substring(0, 1).toUpperCase + name.substring(1)
    exchanges += name -> context.actorOf(
      Exchange.props(
        name,
        AppConfig.exchange(name),
        context.actorOf(exchangeInit.dataChannelProps.apply(), s"$camelName-Exchange"),
        exchangeInit.tpDataChannelProps
      ), camelName)
  }

  def waitUntilExchangesRunning(): Unit = {
    implicit val timeout: Timeout = config.internalCommunicationTimeout
    var in: Set[String] = null
    do {
      Thread.sleep(1000)
      in = Set()
      for (name <- exchanges.keys) {
        try {
          if (Await.result((exchanges(name) ? IsInitialized).mapTo[IsInitializedResponse], timeout.duration).initialized) {
            in += name
          }
        } catch {
          case e: concurrent.TimeoutException => // ignore
        }
      }
      log.info(s"Initialized exchanges: (${in.size}/${exchanges.keySet.size}) : succeded: $in")
    } while (in != exchanges.keySet)
    log.info(s"${Emoji.Satisfied} All exchanges initialized")
  }

  def initExchanges(): Unit = {
    for (name: String <- AppConfig.activeExchanges) {
      initExchange(name, GlobalConfig.AllExchanges(name))
    }
  }

  def initTraders(): Unit = {
    traders += "FooTrader" -> context.actorOf(FooTrader.props(AppConfig.trader("foo-trader"), self), "FooTrader")
  }

  override def preStart(): Unit = {
    initExchanges()
    initTraders()
  }

  def receive: Receive = {

    // messages from Trader

    case GetTradeDecisionContext() =>
      pullFreshTradeDecisionContext() match {
        case Some(dc) =>
          sender() ! dc
        case None =>
      }

    case ob: OrderBundle =>
      if (checkBalanceSufficient(ob)) { // this is the safety-check, the original check should have been done by trader already before
        placeOrderBundleOrders(ob)
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