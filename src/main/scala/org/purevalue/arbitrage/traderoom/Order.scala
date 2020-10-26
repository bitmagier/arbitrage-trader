package org.purevalue.arbitrage.traderoom

import java.time.Instant
import java.util.UUID

import akka.event.Logging
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom.exchange.{ExchangeAccountStreamData, Ticker}
import org.purevalue.arbitrage.util.IncomingDataError
import org.purevalue.arbitrage.util.Util.formatDecimal


/**
 * A (real) order, which comes from exchange data feed
 *
 * @see https://academy.binance.com/economics/what-are-makers-and-takers
 */

case class Order(externalId: String,
                 exchange: String,
                 pair: TradePair,
                 side: TradeSide,
                 orderType: OrderType,
                 price: Option[Double], // may not be there for MARKET orders
                 stopPrice: Option[Double], // [candidate for removal] for STOP_LIMIT - but i've not found that attribute on all exchanges
                 creationTime: Instant,
                 @volatile var quantity: Double,
                 @volatile var orderStatus: OrderStatus,
                 @volatile var cumulativeFilledQuantity: Option[Double],
                 @volatile var priceAverage: Option[Double],
                 @volatile var lastUpdateTime: Instant) {
  private val log = Logging(actorSystem.eventStream, getClass)

  def shortDesc: String = {
    val direction: String = side match {
      case TradeSide.Buy => "<-"
      case TradeSide.Sell => "->"
    }
    s"[$exchange $side ${cumulativeFilledQuantity.map(formatDecimal(_, pair.baseAsset.defaultFractionDigits))} " +
      s"${pair.baseAsset.officialSymbol}$direction${pair.quoteAsset.officialSymbol} " +
      s"""filled ${if (cumulativeFilledQuantity.isDefined && priceAverage.isDefined) formatDecimal(cumulativeFilledQuantity.get * priceAverage.get, pair.quoteAsset.defaultFractionDigits) else "n/a"} """ +
      s"price ${price.map(formatDecimal(_, pair.quoteAsset.defaultFractionDigits))} $orderStatus]"
  }


  // from cointracking.freshdesk.com/en/support/solutions/articles/29000021505-bnb-balance-wrong-due-to-fees-not-being-deducted-
  // In case of a sell, the fee needs to be entered as additional amount on the sell side.
  // In case of a buy, the fee needs to be subtracted from the buy side.

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity(feeRate: Double): LocalCryptoValue = side match {
    case TradeSide.Buy =>
      if (Seq(OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED).contains(orderStatus))
        LocalCryptoValue(
          exchange,
          pair.quoteAsset,
          (priceAverage, cumulativeFilledQuantity) match {
            case (Some(p), Some(q)) => p * q
            case (Some(p), None) => p * quantity
            case (None, Some(q)) if price.isDefined => price.get * q
            case _ if price.isDefined => price.get * quantity
            // if something falls through here, we have to avoid using this function in this case
          })
      else LocalCryptoValue(exchange, pair.quoteAsset, 0.0)

    case TradeSide.Sell =>
      if (Seq(OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED).contains(orderStatus))
        LocalCryptoValue(
          exchange,
          pair.baseAsset,
          cumulativeFilledQuantity match {
            case Some(q) => q * (1.0 + feeRate)
            case None => quantity * (1.0 + feeRate)
          })
      else LocalCryptoValue(exchange, pair.baseAsset, 0.0)
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity(feeRate: Double): LocalCryptoValue = side match {
    case TradeSide.Buy =>
      if (Seq(OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED).contains(orderStatus))
        LocalCryptoValue(exchange,
          pair.baseAsset,
          cumulativeFilledQuantity match {
            case Some(q) => q * (1.0 - feeRate)
            case None => quantity * (1.0 - feeRate)
          })
      else LocalCryptoValue(exchange, pair.baseAsset, 0.0)

    case TradeSide.Sell =>
      if (Seq(OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED).contains(orderStatus))
        LocalCryptoValue(
          exchange,
          pair.quoteAsset,
          (priceAverage, cumulativeFilledQuantity) match {
            case (Some(p), Some(q)) => p * q
            case (Some(p), None) => p * quantity
            case (None, Some(q)) if price.isDefined => price.get * q
            case _ if price.isDefined => price.get * quantity
            // if something falls through here, we have to avoid using this function in this case
          })
      else LocalCryptoValue(exchange, pair.quoteAsset, 0.0)
  }

  def ref: TradeRoom.OrderRef = OrderRef(exchange, pair, externalId)

  def applyUpdate(u: OrderUpdate): Unit = {
    if (u.exchange != exchange ||
      u.externalOrderId != externalId ||
      u.pair != pair ||
      u.side != side ||
      (u.orderType.isDefined && u.orderType.get != orderType))
      throw new IllegalArgumentException(s"$u does not match \n$this")

    if (u.updateTime.isBefore(lastUpdateTime)) {
      log.debug(s"Ignoring $u, because updateTime is not after order's lastUpdateTime: $this")
    } else if (u.updateTime.equals(lastUpdateTime) && orderStatus.isFinal)
      log.debug(s"Ignoring $u, because it has same timestamp and we already have a final order-status here")
    else {
      if (u.orderStatus.isDefined) orderStatus = u.orderStatus.get
      if (u.priceAverage.isDefined) priceAverage = u.priceAverage
      if (u.originalQuantity.isDefined) quantity = u.originalQuantity.get
      cumulativeFilledQuantity = u.cumulativeFilledQuantity match {
        case Some(q) => Some(q)
        case None if u.remainingQuantity.isDefined => Some(quantity - u.remainingQuantity.get)
        case _ => cumulativeFilledQuantity
      }
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
}

sealed trait OrderStatus {
  def isFinal: Boolean
}
sealed trait IntermediateOrderStatus extends OrderStatus {
  override def isFinal: Boolean = false
}
sealed trait FinalOrderStatus extends OrderStatus {
  override def isFinal: Boolean = true
}
object OrderStatus {
  case object NEW extends IntermediateOrderStatus
  case object PARTIALLY_FILLED extends IntermediateOrderStatus
  case object FILLED extends FinalOrderStatus
  case object CANCELED extends FinalOrderStatus // cancelled by user
  case object EXPIRED extends FinalOrderStatus // order was canceled acording to order type's rules
  case object REJECTED extends FinalOrderStatus
  case object PAUSE extends IntermediateOrderStatus
}

/**
 * Order update coming from exchange data flow
 */
case class OrderUpdate(externalOrderId: String,
                       exchange: String,
                       pair: TradePair,
                       side: TradeSide,
                       orderType: Option[OrderType],
                       orderPrice: Option[Double], // may be the original price or a rounded one from the exchange. May not be there for market orders
                       stopPrice: Option[Double],
                       originalQuantity: Option[Double],
                       orderCreationTime: Option[Instant],
                       orderStatus: Option[OrderStatus],
                       cumulativeFilledQuantity: Option[Double],
                       remainingQuantity: Option[Double], // on coinbase we get that insteas of a cumulativeFilledQuantity
                       priceAverage: Option[Double],
                       updateTime: Instant) extends ExchangeAccountStreamData {

  if ((originalQuantity.isDefined && originalQuantity.get < 0.0) ||
    cumulativeFilledQuantity.isDefined && cumulativeFilledQuantity.get < 0.0)
    throw new IncomingDataError("quantity negative")

  def toOrder: Order = Order(
    externalOrderId,
    exchange,
    pair,
    side,
    orderType.get, // shall and will fail when an OrderUpdate without orderType is used for toOrder
    orderPrice,
    stopPrice,
    orderCreationTime.getOrElse(Instant.now),
    (originalQuantity, cumulativeFilledQuantity) match {
      case (Some(oq), _) => oq
      case (_, Some(cq)) if orderStatus.exists(_.isFinal) => cq
      case _ => 0.0
    },
    orderStatus.getOrElse(originalQuantity match { // defaults in case not order status is available
      case Some(originalQuantity) if cumulativeFilledQuantity.isDefined && cumulativeFilledQuantity.get == originalQuantity => OrderStatus.FILLED
      case Some(originalQuantity) if cumulativeFilledQuantity.isDefined && cumulativeFilledQuantity.get < originalQuantity => OrderStatus.PARTIALLY_FILLED
      case Some(_) => OrderStatus.NEW
      case None => OrderStatus.NEW
    }),
    cumulativeFilledQuantity match {
      case Some(q) => Some(q)
      case None if originalQuantity.isDefined && remainingQuantity.isDefined => Some(originalQuantity.get - remainingQuantity.get)
      case _ => None
    },
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
                        orderBundleId: Option[UUID], // mandatory for arbitrage orders, empty for liquidity tx orders
                        exchange: String,
                        pair: TradePair,
                        side: TradeSide,
                        feeRate: Double,
                        amountBaseAsset: Double,
                        limit: Double) {
  def tradeDesc: String = {
    val orderDesc = side match {
      case TradeSide.Buy => s"${pair.baseAsset.officialSymbol}<-${pair.quoteAsset.officialSymbol}"
      case TradeSide.Sell => s"${pair.baseAsset.officialSymbol}->${pair.quoteAsset.officialSymbol}"
    }
    s"($exchange: ${formatDecimal(amountBaseAsset, pair.baseAsset.defaultFractionDigits)} " +
      s"$orderDesc ${formatDecimal(amountBaseAsset * limit, pair.quoteAsset.defaultFractionDigits)} " +
      s"limit ${formatDecimal(limit, pair.quoteAsset.defaultFractionDigits)})"
  }

  def shortDesc: String = s"OrderRequest$tradeDesc"


  override def toString: String = s"OrderRequest($id, orderBundleId:$orderBundleId, $exchange, $pair, $side, $feeRate, " +
    s"amountBaseAsset:${formatDecimal(amountBaseAsset)}, limit:${formatDecimal(limit)})"

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity: LocalCryptoValue = side match {
    case TradeSide.Buy => LocalCryptoValue(exchange, pair.quoteAsset, limit * amountBaseAsset)
    case TradeSide.Sell => LocalCryptoValue(exchange, pair.baseAsset, amountBaseAsset * (1.0 + feeRate))
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity: LocalCryptoValue = side match {
    case TradeSide.Buy => LocalCryptoValue(exchange, pair.baseAsset, amountBaseAsset * (1.0 - feeRate))
    case TradeSide.Sell => LocalCryptoValue(exchange, pair.quoteAsset, limit * amountBaseAsset)
  }
}

/**
 * The bill which is expected from executing an OrderRequest
 * (direct costs only, not including liquidity TXs)
 */
case class OrderBill(balanceSheet: Seq[LocalCryptoValue], sumUSDAtCalcTime: Double) {
  override def toString: String = s"""OrderBill(balanceSheet:[${balanceSheet.mkString(", ")}], sumUSD:${formatDecimal(sumUSDAtCalcTime, 2)})"""
}
object OrderBill {
  /**
   * Calculates the balance sheet of that order
   * incoming value have positive amount; outgoing value have negative amount
   */
  def calcBalanceSheet(order: OrderRequest): Seq[LocalCryptoValue] = {
    Seq(order.calcIncomingLiquidity, order.calcOutgoingLiquidity.negate)
  }

  def calcBalanceSheet(order: Order, feeRate: Double): Seq[LocalCryptoValue] = {
    Seq(order.calcIncomingLiquidity(feeRate), order.calcOutgoingLiquidity(feeRate).negate)
  }

  def aggregateValues(balanceSheet: Iterable[LocalCryptoValue],
                      targetAsset: Asset,
                      findConversionRate: (String, TradePair) => Option[Double]): Double = {

    val sumByLocalAsset: Iterable[LocalCryptoValue] = balanceSheet
      .groupBy(e => (e.exchange, e.asset))
      .map(e => LocalCryptoValue(e._1._1, e._1._2, e._2.map(_.amount).sum))

    sumByLocalAsset
      .map(_.convertTo(targetAsset, findConversionRate).amount)
      .sum
  }

  def aggregateValues(balanceSheet: Iterable[LocalCryptoValue],
                      targetAsset: Asset,
                      tickers: Map[String, Map[TradePair, Ticker]]): Double =
    aggregateValues(balanceSheet, targetAsset, (e, pair) => tickers(e).get(pair).map(_.priceEstimate))

  def calc(orders: Seq[OrderRequest],
           aggregateUSDxAsset: Asset,
           referenceTicker: Map[TradePair, Ticker]): OrderBill = {
    if (!Asset.UsdEquivalentCoins.contains(aggregateUSDxAsset)) throw new IllegalArgumentException("not a USD equivalent asset")

    val balanceSheet: Seq[LocalCryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSD: Double = aggregateValues(balanceSheet, aggregateUSDxAsset, (_, tradePair) => referenceTicker.get(tradePair).map(_.priceEstimate))
    OrderBill(balanceSheet, sumUSD)
  }

  def calc(orders: Seq[Order],
           referenceTicker: collection.Map[TradePair, Ticker],
           aggregateUSDxAsset: Asset,
           feeRates: Map[String, Double]): OrderBill = {
    if (!Asset.UsdEquivalentCoins.contains(aggregateUSDxAsset)) throw new IllegalArgumentException("not a USD equivalent asset")

    val balanceSheet: Seq[LocalCryptoValue] =
      orders.flatMap(o => calcBalanceSheet(o, feeRates(o.exchange)))
    val sumUSD: Double = aggregateValues(balanceSheet, aggregateUSDxAsset, (_, tradePair) => referenceTicker.get(tradePair).map(_.priceEstimate))
    OrderBill(balanceSheet, sumUSD)
  }
}

/** High level order request bundle, covering 2 or more trader orders */
case class OrderRequestBundle(id: UUID,
                              tradePattern: String,
                              creationTime: Instant,
                              orderRequests: List[OrderRequest],
                              bill: OrderBill) {
  def tradeDesc: String = s"""[${orderRequests.map(_.tradeDesc).mkString(" & ")}]"""

  def involvedReserveAssets: Set[Asset] = orderRequests.flatMap(e => Seq(e.pair.baseAsset, e.pair.quoteAsset)).toSet

  override def toString: String = s"OrderRequestBundle($id, $tradePattern, creationTime:$creationTime, orders:$orderRequests, $bill)"
}
