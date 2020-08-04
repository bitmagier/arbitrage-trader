package org.purevalue.arbitrage

import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.GetOrderBooks
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexAdapter
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

sealed trait TradeDirection
object TradeDirection extends TradeDirection {
  case object Buy extends TradeDirection
  case object Sell extends TradeDirection
}

case class CryptoValue(asset: Asset, amount: Double)

/** Order: a single trade request before it's execution */
case class Order(id: UUID,
                 orderBundleId: UUID,
                 exchange: String,
                 tradePair: TradePair,
                 direction: TradeDirection,
                 fee: Fee,
                 amountBaseAsset: Double,
                 amountQuoteAsset: Double,
                 limit: Double) {
  var placed: Boolean = false
  var placementTime: ZonedDateTime = _

  def setPlaced(ts: ZonedDateTime): Unit = {
    placed = true
    placementTime = ts
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
                 supervisorComments: Seq[String])

case class CompletedOrderBundle(orderBundle: OrderBundle, executedAsInstructed: Boolean, executedTrades: Seq[Trade], earnings: Seq[CryptoValue])

object TradeRoom {
  // communication with trader INCOMING
  case class GetTradableAssets()
  /** High level order bundle, covering 2 or more trader orders */
  case class OrderBundle(id: UUID, traderName: String, trader: ActorRef, creationTime: ZonedDateTime, orders: Set[Order], estimatedWin: CryptoValue, decisionInformation: String)
  // communication with trader OUTGOING
  case class TradableAssets(tradable: Map[TradePair, Seq[OrderBook]])
  case class OrderBundlePlaced(orderBundleId: UUID)
  case class OrderBundleCompleted(orderBundle: CompletedOrderBundle)

  // communication with exchange INCOMING
  case class OrderPlaced(orderId: UUID, placementTime: ZonedDateTime)
  case class OrderExecuted(orderId: UUID, trade: Trade)
  case class OrderCancelled(orderId: UUID)
  // communication with exchange OUTGOING
  case class PlaceOrder(order: Order)
  case class CancelOrder()

  def props(): Props = Props(new TradeRoom())
}

/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  var exchanges: Map[String, ActorRef] = Map()
  var traders: Map[String, ActorRef] = Map()

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

    //    traders += "foo" -> context.actorOf(FooTrader.props(StaticConfig.trader("foo")), "foo-trader")
  }

  def calculateEarnings(trades: List[Trade]): Seq[CryptoValue] = {
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

  def collectTradableAssets(): Future[TradableAssets] = {
    implicit val timeout: Timeout = Timeout(2.seconds) // TODO configuration
    var orderBooks = List[Future[List[OrderBook]]]()
    for (exchange <- exchanges.values) {
      orderBooks = (exchange ? GetOrderBooks()).mapTo[List[OrderBook]] :: orderBooks
    }
    val o1: Future[List[OrderBook]] = Future.sequence(orderBooks).map(_.flatten)
    o1.map { l =>
      var result = Map[TradePair, List[OrderBook]]()
      for (ob <- l) {
        result = result + (ob.tradePair -> (ob :: result.getOrElse(ob.tradePair, List())))
      }
      result
    }.map(o3 => TradableAssets(o3))
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

  def orderExecuted(orderId: UUID, trade: Trade): Option[(ActorRef, OrderBundleCompleted)] = {
    var answer: Option[(ActorRef, OrderBundleCompleted)] = None
    val order = activeOrders(orderId)
    activeOrders -= orderId
    val orderBundleId = order.orderBundleId
    tradesPerActiveOrderBundle(orderBundleId) += trade

    val orderIds: Set[UUID] = activeOrderBundles(orderBundleId).orders.map(_.id)
    if (orderIds.forall(orderId => !activeOrders.contains(orderId))) {
      // collect trades and complete OrderBundle
      val trades = tradesPerActiveOrderBundle(orderBundleId).toList
      val completedOrderBundle = CompletedOrderBundle(
        activeOrderBundles(orderBundleId),
        executedAsInstructed = true,
        trades,
        calculateEarnings(trades)
      )
      completedOrderBundles += (orderBundleId -> completedOrderBundle)
      tradesPerActiveOrderBundle -= orderBundleId
      answer = Some((activeOrderBundles(orderBundleId).trader, OrderBundleCompleted(completedOrderBundle)))
      activeOrderBundles -= orderBundleId
    }
    answer
  }

  def receive: Receive = {

    // messages from Trader

    case GetTradableAssets() => collectTradableAssets().pipeTo(sender)
    case t: OrderBundle => placeOrderBundleOrders(t)


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