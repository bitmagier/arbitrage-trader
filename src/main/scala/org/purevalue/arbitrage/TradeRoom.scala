package org.purevalue.arbitrage

import java.time.{Duration, LocalDateTime, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Asset.Bitcoin
import org.purevalue.arbitrage.TradeRoom.{LogStats, OrderBundle, TradeContext}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.purevalue.arbitrage.trader.FooTrader
import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, TimeoutException}

sealed trait TradeSide
object TradeSide extends TradeSide {
  case object Buy extends TradeSide
  case object Sell extends TradeSide
}

case class CryptoValue(asset: Asset, amount: Double) {
  override def toString: String = s"${formatDecimal(amount)} ${asset.officialSymbol}"

  def convertTo(targetAsset: Asset, tc: TradeContext): Option[CryptoValue] = {
    if (this.asset == targetAsset)
      Some(this)
    else {
      // try direct conversion first
      tc.findReferenceTicker(TradePair.of(this.asset, targetAsset)) match {
        case Some(ticker) =>
          Some(CryptoValue(targetAsset, amount * ticker.weightedAveragePrice))
        case None =>
          tc.findReferenceTicker(TradePair.of(targetAsset, this.asset)) match {
            case Some(ticker) =>
              Some(CryptoValue(targetAsset, amount / ticker.weightedAveragePrice))
            case None =>
              None
              if ((this.asset != Bitcoin && targetAsset != Bitcoin)
                && tc.findReferenceTicker(TradePair.of(this.asset, Bitcoin)).isDefined
                && tc.findReferenceTicker(TradePair.of(targetAsset, Bitcoin)).isDefined) {
                this.convertTo(Bitcoin, tc).get.convertTo(targetAsset, tc)
              } else {
                None
              }
          }
      }
    }
  }
}

/** Order: a single trade request before it's execution */
case class Order(id: UUID,
                 orderBundleId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 direction: TradeSide,
                 fee: Fee,
                 amountBaseAsset: Double,
                 limit: Double) {
  override def toString: String = s"Order($id, orderBundleId:$orderBundleId, $exchange, $tradePair, $direction, $fee, " +
    s"amountBaseAsset:${formatDecimal(amountBaseAsset)}, limit:${formatDecimal(limit)})"
}

/** Trade: a successfully executed Order */
case class Trade(id: UUID,
                 orderId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 direction: TradeSide,
                 fee: Fee,
                 amountBaseAsset: Double,
                 amountQuoteAsset: Double,
                 executionRate: Double,
                 executionTime: ZonedDateTime,
                 wasMaker: Boolean,
                 supervisorComments: Seq[String]) // comments from TradeRoom "operator"

case class OrderBill(balanceSheet: Seq[CryptoValue], sumUSDT: Double) {
  override def toString: String = s"OrderBill(balanceSheet:$balanceSheet, sumUSDT:${formatDecimal(sumUSDT)})"
}
object OrderBill {
  def calcBalanceSheet(order: Order): Seq[CryptoValue] = {
    if (order.direction == TradeSide.Buy)
      Seq(
        CryptoValue(order.tradePair.baseAsset, order.amountBaseAsset),
        CryptoValue(order.tradePair.quoteAsset, -(1.0d + order.fee.takerFee) * order.amountBaseAsset * order.limit),
      )
    else if (order.direction == TradeSide.Sell) {
      Seq(
        CryptoValue(order.tradePair.baseAsset, -order.amountBaseAsset),
        CryptoValue(order.tradePair.quoteAsset, (1.0d - order.fee.takerFee) * order.amountBaseAsset * order.limit)
      )
    } else throw new IllegalArgumentException()
  }

  def aggregateValues(balanceSheet: Seq[CryptoValue], targetAsset: Asset, tc: TradeContext): Double = {
    val sumByAsset: Iterable[CryptoValue] = balanceSheet
      .groupBy(_.asset)
      .map(e => CryptoValue(e._1, e._2.map(_.amount).sum))

    sumByAsset.map { v =>
      v.convertTo(targetAsset, tc) match {
        case Some(v) => v.amount
        case None =>
          val conversionOptions: Set[TradePair] = tc.extendedTickers.values.flatMap(_.keySet).toSet
          throw new RuntimeException(s"Unable to convert ${v.asset} to $targetAsset. Here are our total conversion options: $conversionOptions")
      }
    }.sum
  }

  def calc(orders: Seq[Order], tc: TradeContext): OrderBill = {
    val balanceSheet: Seq[CryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSDT: Double = aggregateValues(balanceSheet, Asset("USDT"), tc)
    OrderBill(balanceSheet, sumUSDT)
  }
}

case class CompletedOrderBundle(orderBundle: OrderBundle,
                                executedAsInstructed: Boolean,
                                executedTrades: Seq[Trade],
                                cancelledOrders: Seq[Order],
                                bill: OrderBill)

object TradeRoom {

  /**
   * An always-uptodate view on the TradeRoom Pre-Trade Data.
   * Modification of the content is NOT permitted by users of the TRadeContext (even if technically possible)!
   */
  case class TradeContext(tickers: Map[String, Map[TradePair, Ticker]],
                          extendedTickers: Map[String, Map[TradePair, ExtendedTicker]],
                          orderBooks: Map[String, Map[TradePair, OrderBook]],
                          balances: Map[String, Wallet],
                          fees: Map[String, Fee]) {

    /**
     * find the best ticker stats for the tradepair prioritized by exchange via config
     */
    def findReferenceTicker(tradePair: TradePair): Option[ExtendedTicker] = {
      for (exchange <- AppConfig.tradeRoom.extendedTickerExchanges) {
        extendedTickers.get(exchange) match {
          case Some(eTickers) => eTickers.get(tradePair) match {
            case Some(ticker) => return Some(ticker)
            case _ =>
          }
          case _ =>
        }
      }
      None
    }
  }

  /** High level order bundle, covering 2 or more trader orders */
  case class OrderBundle(id: UUID,
                         traderName: String,
                         trader: ActorRef,
                         creationTime: LocalDateTime,
                         orders: Seq[Order],
                         bill: OrderBill) {
    override def toString: String = s"OrderBundle($id, $traderName, creationTime:$creationTime, orders:$orders, $bill)"
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

  case class LogStats()

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

  private var exchanges: Map[String, ActorRef] = Map()
  private var traders: Map[String, ActorRef] = Map()

  // a map per exchange
  private val tickers: concurrent.Map[String, concurrent.Map[TradePair, Ticker]] = TrieMap()
  private val extendedTickers: concurrent.Map[String, concurrent.Map[TradePair, ExtendedTicker]] = TrieMap()
  private val orderBooks: concurrent.Map[String, concurrent.Map[TradePair, OrderBook]] = TrieMap()
  private val wallets: concurrent.Map[String, Wallet] = TrieMap()

  private val fees: Map[String, Fee] = Map( // TODO
    "binance" -> Fee("binance", AppConfig.exchange("binance").makerFee, AppConfig.exchange("binance").takerFee),
    "bitfinex" -> Fee("bitfinex", AppConfig.exchange("bitfinex").makerFee, AppConfig.exchange("bitfinex").takerFee)
  )

  private val tradeContext: TradeContext = TradeContext(tickers, extendedTickers, orderBooks, wallets, fees)

  //  private var activeOrders: Map[UUID, Order] = Map() // orderId -> Order; orders, belonging to activeOrderBundles & active at the corresponding exchange
  //  private var tradesPerActiveOrderBundle: Map[UUID, ListBuffer[Trade]] = Map() // orderBundleId -> Trade; trades, belonging to activeOrderBundles & executed at an exchange
  //  private var activeOrderBundles: Map[UUID, OrderBundle] = Map()

  // TODO buffer+persist completed order bundles to a database instead (cassandra?)
  //  private var completedOrderBundles: Map[UUID, CompletedOrderBundle] = Map() // orderBundleID -> CompletedOrderBundle

  val scheduleRate: FiniteDuration = FiniteDuration(config.statsInterval.toNanos, TimeUnit.NANOSECONDS)
  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(30.seconds, scheduleRate, self, LogStats())


  def calculateBill(trades: List[Trade]): Seq[CryptoValue] = {
    val rawInvoice = ArrayBuffer[(Asset, Double)]()
    for (trade <- trades) {
      val feeRate = if (trade.wasMaker) trade.fee.makerFee else trade.fee.takerFee
      if (trade.direction == TradeSide.Buy) {
        rawInvoice += ((trade.tradePair.baseAsset, trade.amountBaseAsset.abs))
        rawInvoice += ((trade.tradePair.quoteAsset, -trade.amountQuoteAsset.abs))
        rawInvoice += ((trade.tradePair.quoteAsset, -(trade.amountQuoteAsset.abs * feeRate)))
      } else if (trade.direction == TradeSide.Sell) {
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

  def orderLimitCloseToTicker(order: Order): Boolean = {
    val ticker = tickers(order.exchange)(order.tradePair)
    val bestOfferPrice: Double = if (order.direction == TradeSide.Buy) ticker.lowestAskPrice else ticker.highestBidPrice
    val diff = ((order.limit - bestOfferPrice) / bestOfferPrice).abs
    val valid = diff < config.orderValidityGuard.maxOrderLimitTickerVariance
    if (!valid) {
      log.warn(s"${Emoji.Disagree} Got OrderBundle where a order limit is too far away ($diff) from ticker value " +
        s"(max variance=${formatDecimal(config.orderValidityGuard.maxOrderLimitTickerVariance)})")
      log.debug(s"$order, $ticker")
    }
    valid
  }

  def tickerDataUpToDate(o: Order): Boolean = {
    val r = Duration.between(
      tickers(o.exchange)(o.tradePair).lastUpdated,
      LocalDateTime.now
    ).compareTo(config.orderValidityGuard.maxTickerAge) < 0
    if (!r) {
      log.warn(s"${Emoji.NoSupport} Sorry, can't let that order through, because we don't have an up-to-date ticker here for ${o.exchange} ${o.tradePair} here.")
      log.debug(s"${Emoji.NoSupport} $o")
    }
    r
  }

  def validityGuard(t: OrderBundle): Boolean = {
    if (t.bill.sumUSDT <= 0) {
      log.warn(s"${Emoji.Disagree} Got OrderBundle with negative balance: ${t.bill.sumUSDT}. I will not execute that one!")
      log.debug(s"$t")
      false
    } else if (!t.orders.forall(tickerDataUpToDate)) {
      false
    } else if (t.bill.sumUSDT >= config.orderValidityGuard.maximumReasonableWinPerOrderBundleUSDT) {
      log.warn(s"${Emoji.Disagree} Got OrderBundle with unbelievable high estimated win of ${formatDecimal(t.bill.sumUSDT)} USDT. I will rather not execute that one - seem to be a bug!")
      log.debug(s"${Emoji.Disagree} $t")
      false
    } else if (!t.orders.forall(orderLimitCloseToTicker)) {
      false
    } else {
      true
    }
  }

  def placeOrderBundleOrders(t: OrderBundle): Unit = {
    if (validityGuard(t)) {
      log.info(s"${Emoji.Excited} [simulated] Placing OrderBundle: $t")
    }
  }

  def initExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    val camelName = exchangeName.substring(0, 1).toUpperCase + exchangeName.substring(1)
    tickers += exchangeName -> concurrent.TrieMap[TradePair, Ticker]()
    extendedTickers += exchangeName -> concurrent.TrieMap[TradePair, ExtendedTicker]()
    orderBooks += exchangeName -> concurrent.TrieMap[TradePair, OrderBook]()
    wallets += exchangeName -> Wallet(Map())

    exchanges += exchangeName -> context.actorOf(
      Exchange.props(
        exchangeName,
        AppConfig.exchange(exchangeName),
        context.actorOf(exchangeInit.dataChannelProps.apply(), s"$camelName-Exchange"),
        exchangeInit.tpDataChannelProps,
        TPData(
          tickers(exchangeName),
          extendedTickers(exchangeName),
          orderBooks(exchangeName),
          wallets(exchangeName)
        )
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
          if (Await.result((exchanges(name) ? Exchange.IsInitialized).mapTo[Exchange.IsInitializedResponse], timeout.duration).initialized) {
            in += name
          }
        } catch {
          case e: TimeoutException => // ignore
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
    traders += "FooTrader" -> context.actorOf(
      FooTrader.props(AppConfig.trader("foo-trader"), self, tradeContext),
      "FooTrader")
  }

  override def preStart(): Unit = {
    initExchanges()
    initTraders()
  }


  def logStats(): Unit = {
    def toEntriesPerExchange[T](m: Map[String, Map[TradePair, T]]): String = {
      m.map(e => (e._1, e._2.values.size))
        .toSeq
        .sortBy(_._1)
        .map(e => s"${e._1}:${e._2}")
        .mkString(", ")
    }

    wallets.map { case (exchange, b) => (
      exchange,
      CryptoValue(
        Bitcoin, // conversion to BTC is expected to work ALWAYS!
        b.balances
          .map(e => CryptoValue(e._2.asset, e._2.amountAvailable).convertTo(Bitcoin, tradeContext).get)
          .map(_.amount)
          .sum
      ))
    }
    log.info(s"Total available balances: ")

    log.info(s"${Emoji.Robot} TradeRoom stats: [general] " +
      s"ticker:[${toEntriesPerExchange(tickers)}], " +
      s"/ extendedTicker:[${toEntriesPerExchange(extendedTickers)}], " +
      s"/ orderBooks:[${toEntriesPerExchange(orderBooks)}]")
    if (config.orderBooksEnabled) {
      val orderBookTop3 = orderBooks.flatMap(_._2.values)
        .map(e => (e.bids.size + e.asks.size, e))
        .toSeq
        .sortBy(_._1)
        .reverse
        .take(3)
        .map(e => s"[${e._2.bids.size} bids/${e._2.asks.size} asks: ${e._2.exchange}:${e._2.tradePair}] ")
        .toList
      log.info(s"${Emoji.Robot} TradeRoom stats: [biggest 3 orderbooks] : $orderBookTop3")
      val orderBookBottom3 = orderBooks.flatMap(_._2.values)
        .map(e => (e.bids.size + e.asks.size, e))
        .toSeq
        .sortBy(_._1)
        .take(3)
        .map(e => s"[${e._2.bids.size} bids/${e._2.asks.size} asks: ${e._2.exchange}:${e._2.tradePair}] ")
        .toList
      log.info(s"${Emoji.Robot} TradeRoom stats: [smallest 3 orderbooks] : $orderBookBottom3")
    }
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  def receive: Receive = {

    // messages from Trader

    case ob: OrderBundle =>
      placeOrderBundleOrders(ob)

    case LogStats() =>
      logStats()

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO shudown app in case of serious exceptions
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
// TODO later check order books of opposide trade direction - assure we have exactly one order book per exchange and 2 assets