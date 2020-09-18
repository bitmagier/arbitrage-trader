package org.purevalue.arbitrage.traderoom

import java.time.{Instant, LocalDateTime}
import java.util.UUID

import akka.actor.ActorRef
import org.purevalue.arbitrage.adapter.{ExchangeAccountStreamData, Fee}
import org.purevalue.arbitrage.traderoom.Asset.USDT
import org.purevalue.arbitrage.traderoom.TradeRoom.{OrderRef, TickersReadonly}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.slf4j.LoggerFactory


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
  def shortDesc: String = {
    val direction: String = side match {
      case TradeSide.Buy => "<-"
      case TradeSide.Sell => "->"
    }
    s"[$side ${formatDecimal(cumulativeFilledQuantity, tradePair.baseAsset.defaultPrecision)} " +
      s"${tradePair.baseAsset.officialSymbol}$direction${tradePair.quoteAsset.officialSymbol} " +
      s"${formatDecimal(cumulativeFilledQuantity * priceAverage, tradePair.quoteAsset.defaultPrecision)}]"
  }


  // from cointracking.freshdesk.com/en/support/solutions/articles/29000021505-bnb-balance-wrong-due-to-fees-not-being-deducted-
  // In case of a sell, the fee needs to be entered as additional amount on the sell side.
  // In case of a buy, the fee needs to be subtracted from the buy side.

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity(fee: Fee): LocalCryptoValue = side match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.quoteAsset, priceAverage * cumulativeFilledQuantity)
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.baseAsset, cumulativeFilledQuantity * (1.0 + fee.average))
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity(fee: Fee): LocalCryptoValue = side match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.baseAsset, cumulativeFilledQuantity * (1.0 - fee.average))
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.quoteAsset, priceAverage * cumulativeFilledQuantity)
  }

  def ref: TradeRoom.OrderRef = OrderRef(exchange, tradePair, externalId)

  private val log = LoggerFactory.getLogger(classOf[Order])

  def applyUpdate(u: OrderUpdate): Unit = {
    if (u.exchange != exchange || u.externalOrderId != externalId || u.tradePair != tradePair || u.side != side || u.orderType != orderType) throw new IllegalArgumentException()
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
                       exchange:String,
                       tradePair: TradePair,
                       side: TradeSide,
                       orderType: OrderType,
                       orderPrice: Double,
                       stopPrice: Option[Double],
                       originalQuantity: Option[Double],
                       orderCreationTime: Option[Instant],
                       orderStatus: OrderStatus,
                       cumulativeFilledQuantity: Double,
                       priceAverage: Double,
                       updateTime: Instant) extends ExchangeAccountStreamData {
  def toOrder: Order = Order(
    externalOrderId,
    exchange,
    tradePair,
    side,
    orderType,
    orderPrice,
    stopPrice,
    originalQuantity.getOrElse(0.0), // TODO
    None,
    orderCreationTime.getOrElse(Instant.now),
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
                        orderBundleId: Option[UUID], // mandatory for arbitrage orders, empty for liquidity tx orders
                        exchange: String,
                        tradePair: TradePair,
                        tradeSide: TradeSide,
                        fee: Fee,
                        amountBaseAsset: Double,
                        limit: Double) {
  def tradeDesc: String = {
    val orderDesc = tradeSide match {
      case TradeSide.Buy => s"${tradePair.baseAsset.officialSymbol}<-${tradePair.quoteAsset.officialSymbol}"
      case TradeSide.Sell => s"${tradePair.baseAsset.officialSymbol}->${tradePair.quoteAsset.officialSymbol}"
    }
    s"($exchange: ${formatDecimal(amountBaseAsset, tradePair.baseAsset.defaultPrecision)} " +
      s"$orderDesc ${formatDecimal(amountBaseAsset * limit, tradePair.quoteAsset.defaultPrecision)})"
  }
  def shortDesc: String = s"OrderRequest($exchange: $tradeDesc)"


  override def toString: String = s"OrderRequest($id, orderBundleId:$orderBundleId, $exchange, $tradePair, $tradeSide, $fee, " +
    s"amountBaseAsset:${formatDecimal(amountBaseAsset)}, limit:${formatDecimal(limit)})"

  // absolute (positive) amount minus fees
  def calcOutgoingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset)
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset * (1.0 + fee.average))
  }

  // absolute (positive) amount minus fees
  def calcIncomingLiquidity: LocalCryptoValue = tradeSide match {
    case TradeSide.Buy => LocalCryptoValue(exchange, tradePair.baseAsset, amountBaseAsset * (1.0 - fee.average))
    case TradeSide.Sell => LocalCryptoValue(exchange, tradePair.quoteAsset, limit * amountBaseAsset)
  }
}

/**
 * The bill which is expected from executing an OrderRequest
 * (direct costs only, not including liquidity TXs)
 */
case class OrderBill(balanceSheet: Seq[LocalCryptoValue], sumUSDTAtCalcTime: Double) {
  override def toString: String = s"""OrderBill(balanceSheet:[${balanceSheet.mkString(", ")}], sumUSDT:${formatDecimal(sumUSDTAtCalcTime, 2)})"""
}
object OrderBill {
  /**
   * Calculates the balance sheet of that order
   * incoming value have positive amount; outgoing value have negative amount
   */
  def calcBalanceSheet(order: OrderRequest): Seq[LocalCryptoValue] = {
    Seq(order.calcIncomingLiquidity, order.calcOutgoingLiquidity.negate)
  }

  def calcBalanceSheet(order: Order, fee: Fee): Seq[LocalCryptoValue] = {
    Seq(order.calcIncomingLiquidity(fee), order.calcOutgoingLiquidity(fee).negate)
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
                      tickers: TickersReadonly): Double =
    aggregateValues(balanceSheet, targetAsset, (exchange, tradePair) => tickers(exchange).get(tradePair).map(_.priceEstimate))

  def calc(orders: Seq[OrderRequest],
           tickers: TickersReadonly): OrderBill = {
    val balanceSheet: Seq[LocalCryptoValue] = orders.flatMap(calcBalanceSheet)
    val sumUSDT: Double = aggregateValues(balanceSheet, USDT, tickers)
    OrderBill(balanceSheet, sumUSDT)
  }

  def calc(orders: Seq[Order],
           tickers: TickersReadonly,
           fees: Map[String, Fee]): OrderBill = {
    val balanceSheet: Seq[LocalCryptoValue] = orders.flatMap(o => calcBalanceSheet(o, fees(o.exchange)))
    val sumUSDT: Double = aggregateValues(balanceSheet, USDT, tickers)
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
  def tradeDesc: String = s"""[${orderRequests.map(_.tradeDesc).mkString(" & ")}]"""

  def involvedReserveAssets: Set[Asset] = orderRequests.flatMap(e => Seq(e.tradePair.baseAsset, e.tradePair.quoteAsset)).toSet

  override def toString: String = s"OrderRequestBundle($id, $tradePattern, creationTime:$creationTime, orders:$orderRequests, $bill)"
}
