package org.purevalue.arbitrage

import java.time.{LocalDateTime, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange._
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage.Utils.formatDecimal
import org.purevalue.arbitrage.trader.FooTrader
import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, TimeoutException}

sealed trait TradeDirection
object TradeDirection extends TradeDirection {
  case object Buy extends TradeDirection
  case object Sell extends TradeDirection
}

case class CryptoValue(asset: Asset, amount: Double) {
  override def toString: String = s"${formatDecimal(amount)} ${asset.officialSymbol}"

  def convertTo(targetAsset: Asset, tc: TradeContext): Option[Double] = {
    if (this.asset == targetAsset)
      Some(amount)
    else {
      // try direct conversion first
      tc.findReferenceTicker(TradePair.of(this.asset, targetAsset)) match {
        case Some(ticker) =>
          Some(amount * ticker.weightedAveragePrice)
        case None =>
          tc.findReferenceTicker(TradePair.of(targetAsset, this.asset)) match {
            case Some(ticker) =>
              Some(amount / ticker.weightedAveragePrice)
            case None =>
              None
              if ((this.asset != Asset("BTC") && targetAsset != Asset("BTC"))
                && tc.findReferenceTicker(TradePair.of(this.asset, Asset("BTC"))).isDefined
                && tc.findReferenceTicker(TradePair.of(targetAsset, Asset("BTC"))).isDefined) {
                CryptoValue(Asset("BTC"), this.convertTo(Asset("BTC"), tc).get).convertTo(targetAsset, tc)
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
                 direction: TradeDirection,
                 fee: Fee,
                 amountBaseAsset: Double,
                 limit: Double) {
  //  var placed: Boolean = false
  //  var placementTime: ZonedDateTime = _
  //
  //  def setPlaced(ts: ZonedDateTime): Unit = {
  //    placed = true
  //    placementTime = ts
  //  }

  override def toString: String = s"Order($id, orderBundleId:$orderBundleId, $exchange, $tradePair, $direction, $fee, " +
    s"amountBaseAsset:${formatDecimal(amountBaseAsset)}, limit:${formatDecimal(limit)})"
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

case class OrderBill(balanceSheet: Seq[CryptoValue], sumUSDT: Double) {
  override def toString: String = s"OrderBill(balanceSheet:$balanceSheet, sumUSDT:${formatDecimal(sumUSDT)})"
}
object OrderBill {
  def calcBalanceSheet(order: Order): Seq[CryptoValue] = {
    if (order.direction == TradeDirection.Buy)
      Seq(
        CryptoValue(order.tradePair.baseAsset, order.amountBaseAsset),
        CryptoValue(order.tradePair.quoteAsset, -(1.0d + order.fee.takerFee) * order.amountBaseAsset * order.limit),
      )
    else if (order.direction == TradeDirection.Sell) {
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
        case Some(v) => v
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
                         bill: OrderBill,
                         decisionComment: String) {
    override def toString: String = s"OrderBundle($id, $traderName, creationTime:$creationTime, orders:$orders, $bill, $decisionComment)"
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
  private val fees: Map[String, Fee] = Map( // TODO
    "binance" -> Fee("binance", AppConfig.exchange("binance").makerFee, AppConfig.exchange("binance").takerFee),
    "bitfinex" -> Fee("bitfinex", AppConfig.exchange("bitfinex").makerFee, AppConfig.exchange("bitfinex").takerFee)
  )
  private val tradeContext: TradeContext = TradeContext(tickers, extendedTickers, orderBooks, fees)

  //  private var activeOrders: Map[UUID, Order] = Map() // orderId -> Order; orders, belonging to activeOrderBundles & active at the corresponding exchange
  //  private var tradesPerActiveOrderBundle: Map[UUID, ListBuffer[Trade]] = Map() // orderBundleId -> Trade; trades, belonging to activeOrderBundles & executed at an exchange
  //  private var activeOrderBundles: Map[UUID, OrderBundle] = Map()

  // TODO buffer+persist completed order bundles to a database instead (cassandra?)
  //  private var completedOrderBundles: Map[UUID, CompletedOrderBundle] = Map() // orderBundleID -> CompletedOrderBundle

  val scheduleRate: FiniteDuration = FiniteDuration(config.statsInterval.toNanos, TimeUnit.NANOSECONDS)
  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(30.seconds, scheduleRate, self, LogStats())


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

  def validityCheck(t: OrderBundle): Boolean = {
    if (t.bill.sumUSDT <= 0) {
      log.warn(s"${Emoji.Questionable} Got OrderBundle with negative balance: $t. I will not execute that one!")
      false
    } else if (t.bill.sumUSDT >= config.maximumReasonableWinPerOrderBundleUSDT) {
      log.warn(s"${Emoji.EyeRoll} Got OrderBundle with unbelievable high win of ${formatDecimal(t.bill.sumUSDT)} USDT: $t. I will rather not execute that one - seem to be a bug!")
      false
//    } else {
//      // TODO Doublecheck with Ticker best bid/ask + limit
    } else {
      true
    }
  }

  def placeOrderBundleOrders(t: OrderBundle): Unit = {
    if (validityCheck(t)) {
      log.info(s"${Emoji.Excited} [simulated] Placing OrderBundle: $t")
    }
  }

  def initExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    val camelName = exchangeName.substring(0, 1).toUpperCase + exchangeName.substring(1)
    tickers += exchangeName -> concurrent.TrieMap[TradePair, Ticker]()
    extendedTickers += exchangeName -> concurrent.TrieMap[TradePair, ExtendedTicker]()
    orderBooks += exchangeName -> concurrent.TrieMap[TradePair, OrderBook]()

    exchanges += exchangeName -> context.actorOf(
      Exchange.props(
        exchangeName,
        AppConfig.exchange(exchangeName),
        context.actorOf(exchangeInit.dataChannelProps.apply(), s"$camelName-Exchange"),
        exchangeInit.tpDataChannelProps,
        TPData(
          tickers(exchangeName),
          extendedTickers(exchangeName),
          orderBooks(exchangeName)
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
          if (Await.result((exchanges(name) ? IsInitialized).mapTo[IsInitializedResponse], timeout.duration).initialized) {
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
    log.info(s"${Emoji.Robot} TradeRoom stats (1/3) [exchanges / ticker / extended-ticker / orderbooks] : [" +
      s"${exchanges.size} / " +
      s"${tickers.values.map(_.values.count(_ => true))}" +
      s"/ ${extendedTickers.values.map(_.values.count(_ => true))}" +
      s"/ ${orderBooks.values.map(_.values.count(_ => true))}]")
    val orderBookTop3 = orderBooks.flatMap(_._2.values)
      .map(e => (e.bids.size + e.asks.size, e))
      .toSeq
      .sortBy(_._1)
      .reverse
      .take(3)
      .map(e => s"[${e._2.bids.size} bids/${e._2.asks.size} asks: ${e._2.exchange}:${e._2.tradePair}] ")
      .toList
    log.info(s"${Emoji.Robot} TradeRoom stats (2/3) [biggest 3 orderbooks] : $orderBookTop3")
    val orderBookBottom3 = orderBooks.flatMap(_._2.values)
      .map(e => (e.bids.size + e.asks.size, e))
      .toSeq
      .sortBy(_._1)
      .take(3)
      .map(e => s"[${e._2.bids.size} bids/${e._2.asks.size} asks: ${e._2.exchange}:${e._2.tradePair}] ")
      .toList
    log.info(s"${Emoji.Robot} TradeRoom stats (3/3) [smallest 3 orderbooks] : $orderBookBottom3")
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