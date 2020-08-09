package org.purevalue.arbitrage

import java.text.DecimalFormat
import java.time.{Duration, LocalDateTime, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.CryptoValue.formatDecimal
import org.purevalue.arbitrage.Exchange._
import org.purevalue.arbitrage.TradeRoom._
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
      if (this.asset == targetAsset) Some(amount)
      else {
        // try direct conversion first
        tc.findReferenceTicker(TradePair.of(this.asset, targetAsset)) match {
          case Some(ticker) =>
              Some(amount * ticker.weightedAveragePrice)
          case None => // convert to BTC first and then to targetAsset
            val toBtcTicker = tc.findReferenceTicker(TradePair.of(this.asset, Asset("BTC")))
            if (toBtcTicker.isEmpty) return None
            val toBtcRate = toBtcTicker.get.weightedAveragePrice

            val btcToTargetTicker = tc.findReferenceTicker(TradePair.of(Asset("BTC"), targetAsset))
            if (btcToTargetTicker.isEmpty) return None
            val btcToTargetRate = btcToTargetTicker.get.weightedAveragePrice

            Some(amount * toBtcRate * btcToTargetRate)
        }
      }
    }
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
    def findReferenceTicker(tradePair:TradePair): Option[ExtendedTicker] = {
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
                         estimatedWinUSDT: Double,
                         decisionComment: String)

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

  def isUpToDate(ob: OrderBook, now: LocalDateTime): Boolean = {
    Duration.between(ob.lastUpdated, now).compareTo(config.maxDataAge) <= 0
  }

  def isUpToDate(t: ExtendedTicker, now: LocalDateTime): Boolean = {
    Duration.between(t.lastUpdated, now).compareTo(config.maxDataAge) <= 0
  }

  def placeOrderBundleOrders(t: OrderBundle): Unit = {
    log.debug(s"got order bundle to place: $t")
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
    log.info(s"${Emoji.Robot} TradeRoom statistics [exchanges / ticker / orderbooks] : [" +
      s"${exchanges.size} / " +
      s"${tickers.values.flatMap(_.values).count(_ => true)}" +
      s"/ ${orderBooks.values.flatMap(_.values).count(_ => true)}]")
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