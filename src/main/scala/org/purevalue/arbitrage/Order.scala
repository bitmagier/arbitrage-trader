package org.purevalue.arbitrage

import java.time.ZonedDateTime
import java.util.UUID

import org.purevalue.arbitrage.TradeRoom.TradeContext
import org.purevalue.arbitrage.Utils.formatDecimal
import spray.json.JsObject

import scala.collection.mutable.ArrayBuffer

/**
 * OrderRequest: a single trade request before it is sent to an exchange
 */
case class OrderRequest(id: UUID,
                        orderBundleId: UUID,
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
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset * (1.0 + fee.takerFee))
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset)
    case _ => throw new IllegalArgumentException
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset)
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset * (1.0 - fee.takerFee))
    case _ => throw new IllegalArgumentException
  }
}

/**
 * The bill which is expected from executing an OrderRequest
 * (direct costs only, not including liquidity TXs)
 */
case class OrderRequestBill(balanceSheet: Seq[CryptoValue], sumUSDT: Double) {
  override def toString: String = s"OrderRequestBill(balanceSheet:$balanceSheet, sumUSDT:${formatDecimal(sumUSDT)})"
}
object OrderRequestBill {
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

  def aggregateValues(balanceSheet: Iterable[CryptoValue], targetAsset: Asset, tc: TradeContext): Double =
    aggregateValues(balanceSheet, targetAsset, tp => tc.findReferenceTicker(tp).map(_.weightedAveragePrice))

  def calc(orders: Seq[OrderRequest], tc: TradeContext): OrderRequestBill = {
    val balanceSheet: Seq[CryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSDT: Double = aggregateValues(balanceSheet, Asset("USDT"), tc)
    OrderRequestBill(balanceSheet, sumUSDT)
  }
}

sealed trait OrderType
case object LIMIT extends OrderType
case object MARKET extends OrderType
case object STOP_LIMIT extends OrderType
// bitfinex extras: TRAILING_STOP, EXCHANGE_MARKET, EXCHANGE_LIMIT, EXCHANGE_STOP, EXCHANGE_STOP_LIMIT, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC

sealed trait OrderStatus
// bitfinex: ACTIVE, PARTIALLY FILLED, RSN_PAUSE (trading is paused / paused due to AMPL rebase event)

/**
 * A real order, which comes from exchange data feed
 */
case class PlacedOrder(externalId: String,
                       tradePair:TradePair,
                       creationTimestamp:ZonedDateTime,
                       updateTimestamp:ZonedDateTime,
                       side:TradeSide,
                       amount:Double,
                       originalAmount:Double,
                       orderType: OrderType,
                       previousOrderType: OrderType,
                       status: OrderStatus,
                       price: Double,
                       priceAvg: Double,
                       originalJson:JsObject,
                       orderRequest: OrderRequest)

/**
 * Trade: a successfully executed Order
 *
 * The order flow:  OrderRequest -> PlacedOrder -> ExecutedTrade
 *                                         (or) -> FailedOrder
 */
case class ExecutedTrade(externalId: String,
                         exchange: String,
                         tradePair: TradePair,
                         executionTime: ZonedDateTime,
                         externalOrderId: String,
                         side: TradeSide,
                         wasMaker: Boolean,
                         fee: CryptoValue,
                         orderType: OrderType,
                         execAmountBaseAsset: Double,
                         execPrice: Double,
                         originalJson:JsObject,
                         placedOrder: PlacedOrder,
                         supervisorComments: Seq[String]) // comments from TradeRoom "operator"
