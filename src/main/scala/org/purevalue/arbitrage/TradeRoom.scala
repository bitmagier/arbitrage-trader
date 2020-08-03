package org.purevalue.arbitrage

import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.TradeRoom.{GetTradableAssets, OrderBundle, OrderBundleCompleted, OrderBundlePlaced, OrderExecuted, OrderPlaced, PlaceOrder}
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexAdapter
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

trait TradeDirection
case class Buy() extends TradeDirection
case class Sell() extends TradeDirection

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
  def setPlaced(ts:ZonedDateTime): Unit = {placed = true; placementTime = ts}
}

/** Trade: a successfully executed Order */
case class Trade(id:UUID,
                 orderId:UUID,
                 exchange:String,
                 tradePair:TradePair,
                 direction: TradeDirection,
                 fee: Fee,
                 amountBaseAsset: Double,
                 amountQuoteAsset:Double,
                 executionRate:Double,
                 executionTime:ZonedDateTime)

case class CompletedOrderBundle(orderBundle: OrderBundle, executedAsInstructed:Boolean, supervisorComments:List[String], executedTrades:List[Trade], earnings:Set[CryptoValue])

object TradeRoom {
  // communication with trader INCOMING
  case class GetTradableAssets()
  /** High level order bundle, covering 2 or more trader orders */
  case class OrderBundle(id: UUID, traderName:String, trader: ActorRef, creationTime:ZonedDateTime, orders: Set[Order], estimatedWin: CryptoValue, decisionInformation:String)
  // communication with trader OUTGOING
  case class TradableAssets(tradable: Map[TradePair, Map[String, OrderBookManager]])
  case class OrderBundlePlaced(orderBundleId: UUID)
  case class OrderBundleCompleted(orderBundle: CompletedOrderBundle)

  // communication with exchange INCOMING
  case class OrderPlaced(orderId:UUID, placementTime:ZonedDateTime)
  case class OrderExecuted(orderId:UUID, trade:Trade)
  case class OrderCancelled(orderId:UUID)
  // communication with exchange OUTGOING
  case class PlaceOrder(order:Order)
  case class CancelOrder()

  def props(): Props = Props(new TradeRoom())
}

// TODO design in progress
/**
 *  - brings exchanges and traders together
 *  - handles open/partial trade execution
 *  - provides higher level (aggregated per order bundle) interface to traders
 *  - manages trade history
 */
class TradeRoom extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TradeRoom])

  var exchanges: Map[String, ActorRef] = Map()
  var traders: Map[String, ActorRef] = Map()

  var activeOrders: Map[UUID, Order] = Map() // orderId -> Order; orders, belonging to activeOrderBundles & active at the corresponding exchange
  var tradesPerActiveOrderBundle: Map[UUID, ListBuffer[Trade]] = Map() // orderBundleId -> Trade; trades, belonging to activeOrderBundles & executed at an exchange
  var activeOrderBundles: Map[UUID, OrderBundle] = Map()

  // TODO persist completed order bundles to a database and cleanup here
  var completedOrderBundles: Map[UUID, CompletedOrderBundle] = Map() // orderBundleID -> CompletedOrderBundle


  override def preStart(): Unit = {
    exchanges += "binance" -> context.actorOf(Exchange.props("binance", StaticConfig.exchange("binance"),
      context.actorOf(BinanceAdapter.props(StaticConfig.exchange("binance")), "BinanceAdapter")), "binance")

    exchanges += "bitfinex" -> context.actorOf(Exchange.props("bitfinex", StaticConfig.exchange("bitfinex"),
      context.actorOf(BitfinexAdapter.props(StaticConfig.exchange("bitfinex")), "BitfinexAdapter")), "bitfinex")

    log.info(s"Initializing exchanges: ${exchanges.keys}")

//    traders += "foo" -> context.actorOf(FooTrader.props(StaticConfig.trader("foo")), "foo-trader")
  }

  def calculateEarnings(trades: List[Trade]): Set[CryptoValue] = ???

  def receive: Receive = {
    case GetTradableAssets() => ???
      // TODO deliver TradeblePairs to sender

    case t:OrderBundle =>
      tradesPerActiveOrderBundle += (t.id -> ListBuffer())
      for (order <- t.orders) {
        exchanges(order.exchange) ! PlaceOrder(order)
      }

    case OrderPlaced(orderId, placementTime) =>
      val order = activeOrders(orderId)
      order.setPlaced(placementTime)
      if (log.isTraceEnabled) log.trace(s"Order placed: $order ")

      if (activeOrderBundles(order.orderBundleId).orders.forall(_.placed)) {
        sender ! OrderBundlePlaced(order.orderBundleId)
      }

      case OrderExecuted(orderId, trade) =>
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
            List(),
            trades,
            calculateEarnings(trades)
          )
          completedOrderBundles += (orderBundleId -> completedOrderBundle)
          tradesPerActiveOrderBundle -= orderBundleId
          activeOrderBundles(orderBundleId).trader ! OrderBundleCompleted(completedOrderBundle)
          activeOrderBundles -= orderBundleId
        }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// TODO shudown app in case of exception from any actor
// TODO add feature Exchange-PlatformStatus to cover Maintainance periods
// TODO check order books of opposide trade direction - assure we have exactly one order book per exchange and 2 assets