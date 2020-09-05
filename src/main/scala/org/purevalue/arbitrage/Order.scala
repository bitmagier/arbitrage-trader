package org.purevalue.arbitrage

import java.time.{Instant, LocalDateTime}
import java.util.UUID

import akka.actor.ActorRef
import org.purevalue.arbitrage.TradeRoom.{OrderRef, ReferenceTicker}
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

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset * (1.0 + fee.average))
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset)
    case _ => throw new IllegalArgumentException
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset)
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset * (1.0 - fee.average))
    case _ => throw new IllegalArgumentException
  }
}

/**
 * The bill which is expected from executing an OrderRequest
 * (direct costs only, not including liquidity TXs)
 */
case class OrderBill(balanceSheet: Seq[CryptoValue], sumUSDT: Double) {
  override def toString: String = s"OrderRequestBill(balanceSheet:$balanceSheet, sumUSDT:${formatDecimal(sumUSDT, 2)})"
}
object OrderBill {
  /**
   * Calculates the balance sheet of that order
   * incoming value have positive amount; outgoing value have negative amount
   */
  def calcBalanceSheet(order: OrderRequest): Seq[CryptoValue] = {
    val result = ArrayBuffer[CryptoValue]()
    order.calcIncomingLiquidity match {
      case v: LocalCryptoValue => result.append(CryptoValue(v.asset, v.amount))
    }
    order.calcOutgoingLiquidity match {
      case v: LocalCryptoValue => result.append(CryptoValue(v.asset, -v.amount))
    }
    result
  }

  def aggregateValues(balanceSheet: Iterable[CryptoValue], targetAsset: Asset, findReferenceRate: TradePair => Option[Double]): Double = {
    val sumByAsset: Iterable[CryptoValue] = balanceSheet
      .groupBy(_.asset)
      .map(e => CryptoValue(e._1, e._2.map(_.amount).sum))

    sumByAsset.map { v =>
      v.convertTo(targetAsset, findReferenceRate) match {
        case Some(v) => v.amount
        case None =>
          throw new RuntimeException(s"Unable to convert ${v.asset} to $targetAsset")
      }
    }.sum
  }

  def aggregateValues(balanceSheet: Iterable[CryptoValue], targetAsset: Asset, referenceTicker: ReferenceTicker): Double =
    aggregateValues(balanceSheet, targetAsset, tp => referenceTicker.values.get(tp).map(_.currentPriceEstimate))

  def calc(orders: Seq[OrderRequest], referenceTicker: ReferenceTicker): OrderBill = {
    val balanceSheet: Seq[CryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSDT: Double = aggregateValues(balanceSheet, Asset("USDT"), referenceTicker)
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
