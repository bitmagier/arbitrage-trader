package org.purevalue.arbitrage

import java.time.{Instant, LocalDateTime}
import java.util.UUID

import akka.actor.ActorRef
import org.purevalue.arbitrage.TradeRoom.{OrderRef, ReferenceTicker, TickersReadonly}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer


/**
 * A (real) order, which comes from exchange data feed
 */
case class Order(externalId: String,
                 exchange: String,
                 tradePair: TradePair,
                 side: TradeSide,
                 orderType: OrderType,
                 orderPrice: Double,
                 stopPrice: Option[Double], // for STOP_LIMIT
                 quantity: Double, // (TODO check what AMOUNT & AMOUNT_ORIG means on bitfinex)
                 orderRejectReason: Option[String],
                 creationTime: Instant,
                 var orderStatus: OrderStatus,
                 var cumulativeFilledQuantity: Double,
                 var priceAverage: Double,
                 var lastUpdateTime: Instant) extends ExchangeAccountStreamData {
  def ref: TradeRoom.OrderRef = OrderRef(exchange, tradePair, externalId)


  private val log = LoggerFactory.getLogger(classOf[Order])

  def applyUpdate(u: OrderUpdate): Unit = {
    if (u.externalOrderId != externalId || u.tradePair != tradePair || u.side != side || u.orderType != orderType) throw new IllegalArgumentException()
    if (u.updateTime.isBefore(lastUpdateTime)) {
      log.warn(s"Ignoring $u, because updateTime is not after order's lastUpdateTime: $this")
    } else {
      orderStatus = u.orderStatus
      priceAverage = u.priceAverage
      cumulativeFilledQuantity = u.cumulativeFilledQuantity
      lastUpdateTime = u.updateTime
    }
  }
}


sealed trait TradeSide
object TradeSide {
  case object Buy extends TradeSide
  case object Sell extends TradeSide
}

sealed trait OrderType
object OrderType {
  case object LIMIT extends OrderType
  case object MARKET extends OrderType
  case object STOP_LOSS extends OrderType
  case object STOP_LOSS_LIMIT extends OrderType
  case object TAKE_PROFIT extends OrderType // binance only
  case object TAKE_PROFIT_LIMIT extends OrderType // binance only
  case object LIMIT_MAKER extends OrderType // binance only
  // bitfinex extras: TRAILING_STOP, EXCHANGE_MARKET, EXCHANGE_LIMIT, EXCHANGE_STOP, EXCHANGE_STOP_LIMIT, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC
}

sealed case class OrderStatus(isFinal: Boolean)
object OrderStatus {
  object NEW extends OrderStatus(false)
  object PARTIALLY_FILLED extends OrderStatus(false)
  object FILLED extends OrderStatus(true)
  object CANCELED extends OrderStatus(true) // cancelled by user
  object EXPIRED extends OrderStatus(true) // order was canceled acording to order type's rules
  object REJECTED extends OrderStatus(true)
  object PAUSE extends OrderStatus(false)
}

/**
 * Order update coming from exchange data flow
 */
case class OrderUpdate(externalOrderId: String,
                       tradePair: TradePair,
                       side: TradeSide,
                       orderType: OrderType,
                       orderPrice: Double,
                       stopPrice: Option[Double],
                       originalQuantity: Double,
                       orderCreationTime: Instant,
                       orderStatus: OrderStatus,
                       cumulativeFilledQuantity: Double,
                       priceAverage: Double,
                       updateTime: Instant) extends ExchangeAccountStreamData {
  def toOrder(exchange:String): Order = Order(
    externalOrderId,
    exchange,
    tradePair,
    side,
    orderType,
    orderPrice,
    stopPrice,
    originalQuantity,
    None,
    orderCreationTime,
    orderStatus,
    cumulativeFilledQuantity,
    priceAverage,
    updateTime
  )
}

/**
 * OrderRequest: a single trade request before it is sent to an exchange
 *
 * This is the order flow: [Trader] -> OrderRequest -> [Exchange] -> Order(OrderStatus)
 */
case class OrderRequest(id: UUID,
                        orderBundleId: UUID, // nullable TODO: convert to Option
                        exchange: String,
                        tradePair: TradePair,
                        tradeSide: TradeSide,
                        fee: Fee,
                        amountBaseAsset: Double,
                        limit: Double) {

  override def toString: String = s"OrderRequest($id, orderBundleId:$orderBundleId, $exchange, $tradePair, $tradeSide, $fee, " +
    s"amountBaseAsset:${formatDecimal(amountBaseAsset)}, limit:${formatDecimal(limit)})"


  // from cointracking.freshdesk.com/en/support/solutions/articles/29000021505-bnb-balance-wrong-due-to-fees-not-being-deducted-
  // In case of a sell, the fee needs to be entered as additional amount on the sell side.
  // In case of a buy, the fee needs to be subtracted from the buy side.

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset)
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset * (1.0 + fee.average))
    case _ => throw new IllegalArgumentException
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset * (1.0 - fee.average))
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset)
    case _ => throw new IllegalArgumentException
  }
}

/**
 * The bill which is expected from executing an OrderRequest
 * (direct costs only, not including liquidity TXs)
 */
case class OrderBill(balanceSheet: Seq[LocalCryptoValue], sumUSDT: Double) {
  override def toString: String = s"OrderRequestBill(balanceSheet:$balanceSheet, sumUSDT:${formatDecimal(sumUSDT, 2)})"
}
object OrderBill {
  /**
   * Calculates the balance sheet of that order
   * incoming value have positive amount; outgoing value have negative amount
   */
  def calcBalanceSheet(order: OrderRequest): Seq[LocalCryptoValue] = {
    val out = order.calcOutgoingLiquidity
    Seq(order.calcIncomingLiquidity, LocalCryptoValue(out.exchange, out.asset, -out.amount))
  }

  def aggregateValues(balanceSheet: Iterable[LocalCryptoValue],
                      targetAsset: Asset,
                      findConversionRate: (String,TradePair) => Option[Double]): Double = {
    val sumByLocalAsset: Iterable[LocalCryptoValue] = balanceSheet
      .groupBy(e => (e.exchange,e.asset))
      .map(e => LocalCryptoValue(e._1._1, e._1._2, e._2.map(_.amount).sum))

    sumByLocalAsset.map { v =>
      v.convertTo(targetAsset, findConversionRate) match {
        case Some(v) => v.amount
        case None =>
          throw new RuntimeException(s"Unable to convert ${v.asset} to $targetAsset")
      }
    }.sum
  }

  def aggregateValues(balanceSheet: Iterable[LocalCryptoValue],
                      targetAsset: Asset,
                      tickers: TickersReadonly): Double =
    aggregateValues(balanceSheet, targetAsset, (exchange, tradePair) => tickers(exchange).get(tradePair).map(_.priceEstimate))

  def calc(orders: Seq[OrderRequest],
           tickers: TickersReadonly): OrderBill = {
    val balanceSheet: Seq[LocalCryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSDT: Double = aggregateValues(balanceSheet, Asset("USDT"), tickers)
    OrderBill(balanceSheet, sumUSDT)
  }
}

/** High level order request bundle, covering 2 or more trader orders */
case class OrderRequestBundle(id: UUID,
                              tradePattern: String,
                              trader: ActorRef,
                              creationTime: LocalDateTime,
                              orderRequests: List[OrderRequest],
                              bill: OrderBill) {

  def involvedReserveAssets: Set[Asset] = orderRequests.flatMap(e => Seq(e.tradePair.baseAsset, e.tradePair.quoteAsset)).toSet

  override def toString: String = s"OrderRequestBundle($id, $tradePattern, creationTime:$creationTime, orders:$orderRequests, $bill)"
}
