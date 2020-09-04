package org.purevalue.arbitrage

import java.time.{Duration, Instant}
import java.util.UUID

import org.purevalue.arbitrage.TradeRoom.{OrderBundle, ReferenceTicker}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.slf4j.LoggerFactory

sealed trait SafetyGuardDecision
case object Okay extends SafetyGuardDecision
case object NegativeBalance extends SafetyGuardDecision
case object TickerOutdated extends SafetyGuardDecision
case object TooFantasticWin extends SafetyGuardDecision
case object OrderLimitFarAwayFromTicker extends SafetyGuardDecision
case object SameTradePairOrderStillActive extends SafetyGuardDecision
case object TotalTransactionUneconomic extends SafetyGuardDecision

case class OrderBundleSafetyGuard(config: OrderBundleSafetyGuardConfig,
                                  tickers: scala.collection.Map[String, scala.collection.concurrent.Map[TradePair, Ticker]],
                                  dataAge: scala.collection.Map[String, TPDataTimestamps],
                                  referenceTicker: ReferenceTicker,
                                  activeOrderBundles: scala.collection.Map[UUID, OrderBundle]) {
  private val log = LoggerFactory.getLogger(classOf[OrderBundleSafetyGuard])
  private var stats: Map[SafetyGuardDecision, Int] = Map()

  def unsafeStats: Map[SafetyGuardDecision, Int] = stats

  private def orderLimitCloseToTicker(order: OrderRequest): Boolean = {
    val ticker = tickers(order.exchange)(order.tradePair)
    val bestOfferPrice: Double = if (order.tradeSide == TradeSide.Buy) ticker.lowestAskPrice else ticker.highestBidPrice
    val diff = ((order.limit - bestOfferPrice) / bestOfferPrice).abs
    val valid = diff < config.maxOrderLimitTickerVariance
    if (!valid) {
      log.warn(s"${Emoji.Disagree}  Got OrderBundle where an order limit is too far away ($diff) from ticker value " +
        s"(max variance=${formatDecimal(config.maxOrderLimitTickerVariance)})")
      log.debug(s"$order, $ticker")
    }
    valid
  }

  private def tickerDataUpToDate(o: OrderRequest): Boolean = {
    val age = Duration.between(dataAge(o.exchange).tickerTS, Instant.now)
    val r = age.compareTo(config.maxTickerAge) < 0
    if (!r) {
      log.warn(s"${Emoji.NoSupport}  Sorry, can't let that order through, because we have an aged ticker (${age.toSeconds} s) for ${o.exchange} here.")
      log.debug(s"${Emoji.NoSupport}  $o")
    }
    r
  }

  /**
   * Because the liquidity providing and returning to reserve liquidity transactions (done by the Liquidity Manager)
   * may ruin the plan to make a win out of the arbitrage trade bundle,
   * we simulate the following transactions:
   * - Providing Altcoins for the transaction(s) from a non-involved Reserve Asset on the same exchange
   * - Transforming the bought Altcoins back to that Reserve Asset on the same exchange
   * and then calculate the balance
   *
   * For instance:
   * Liquidity reserve Assets are: BTC und USDT
   * TradeBundleTrades are: Exchange1: ETC:BTC Buy   und Exchange2: ETC:BTC Sell  amount=3.5 ETC
   * Altcoin liquidity providing: On Exchange2: Buy 3.5 ETC from USDT         => + 3.5 ETC - 3.5 * (1.0 + fee) * (Rate[ETC:USDT] on Exchange2) USDT
   * Reserve liquidity back-conversion: On Exchange1: Buy USDT from 3.5 ETC   => - 3.5 ETC + 3.5 * (1.0 + fee) * (Rate[ETC:USDT] on Exchange1) USDT
   *
   * It is possible to use different Liquidity reserve assets for different involved assets/trades
   */
  def balanceOfLiquidityTransformationCompensationTransactionsInUSDT(t: OrderRequestBundle): Option[Double] = {
    val allReserveAssets: List[Asset] = Config.liquidityManager.reserveAssets
    val involvedAssets: Set[Asset] = t.orderRequests.flatMap(e => Seq(e.tradePair.baseAsset, e.tradePair.quoteAsset)).toSet
    val uninvolvedReserveAssets: List[Asset] = allReserveAssets.filterNot(involvedAssets.contains)

    val toProvide: Iterable[LocalCryptoValue] = t.orderRequests.map(_.calcOutgoingLiquidity).filterNot(e => allReserveAssets.contains(e.asset))
    val toConvertBack: Iterable[LocalCryptoValue] = t.orderRequests.map(_.calcIncomingLiquidity).filterNot(e => allReserveAssets.contains(e.asset))

    def findUsableReserveAsset(exchange: String, coin: Asset, possibleReserveAssets: List[Asset]): Option[Asset] = {
      possibleReserveAssets.find(r => tickers(exchange).contains(TradePair.of(coin, r)))
    }

    val unableToProvideConversionForCoin: Option[LocalCryptoValue] = {
      (toProvide ++ toConvertBack).find(v => findUsableReserveAsset(v.exchange, v.asset, uninvolvedReserveAssets).isEmpty)
    }
    if (unableToProvideConversionForCoin.isDefined) {
      log.warn(s"${Emoji.EyeRoll}  Sorry, no suitable reserve asset found to support reserve liquidity conversion from/to ${unableToProvideConversionForCoin.get.asset} on ${unableToProvideConversionForCoin.get.exchange}. Concerns $t")
      return None
    }

    val transactions =
      toProvide.map(e => {
        val tradePair = TradePair.of(e.asset, findUsableReserveAsset(e.exchange, e.asset, uninvolvedReserveAssets).get)
        OrderRequest(null, null,
          e.exchange,
          tradePair,
          TradeSide.Buy,
          Config.exchange(e.exchange).fee, // TODO take real values
          e.amount,
          tickers(e.exchange)(tradePair).priceEstimate
        )
      }) ++ toConvertBack.map(e => {
        val tradePair = TradePair.of(e.asset, findUsableReserveAsset(e.exchange, e.asset, uninvolvedReserveAssets).get)
        OrderRequest(null, null,
          e.exchange,
          tradePair,
          TradeSide.Sell,
          Config.exchange(e.exchange).fee, // TODO take real values
          e.amount,
          tickers(e.exchange)(tradePair).priceEstimate
        )
      })

    val balanceSheet: Iterable[CryptoValue] = transactions.flatMap(OrderBill.calcBalanceSheet)

    // self check
    val groupedAndCleanedUpBalanceSheet: Iterable[CryptoValue] =
      balanceSheet
        .groupBy(_.asset)
        .map(e => CryptoValue(e._1, e._2.map(_.amount).sum)) // summed up values of same asset
        .filterNot(_.amount == 0.0d) // ignore zeros

    if (!groupedAndCleanedUpBalanceSheet.forall(v => allReserveAssets.contains(v.asset))) {
      log.error(s"Cleaned up balance sheet should contain reserve asset values only! Instead it is found: $groupedAndCleanedUpBalanceSheet")
      None
    }

    Some(
      OrderBill.aggregateValues(
        groupedAndCleanedUpBalanceSheet,
        Asset.USDT,
        referenceTicker))
  }


  // returns decision-result and the total win in case the decision-result is true
  def totalTransactionsWinInRage(t: OrderRequestBundle): (Boolean, Option[Double]) = {
    val b: Option[Double] = balanceOfLiquidityTransformationCompensationTransactionsInUSDT(t)
    if (b.isEmpty) return (false, None)
    if ((t.bill.sumUSDT + b.get) < config.minTotalGainInUSDT) {
      log.debug(s"${Emoji.LookingDown}  Got interesting $t, but the sum of costs (${formatDecimal(b.get)} USDT) of the necessary " +
        s"liquidity transformation transactions makes the whole thing uneconomic (total gain: ${formatDecimal(t.bill.sumUSDT + b.get)} USDT = lower than threshold ${config.minTotalGainInUSDT} USDT).")
      (false, None)
    } else (true, Some(t.bill.sumUSDT))
  }

  // ^^^ TODO instead of taking only the first possible one, better try all alternatives of reserve liquidity asset conversion before giving up here


  // returns decision result and the total win. in case of a positive decision result
  def isSafe(bundle: OrderRequestBundle): (Boolean, Option[Double]) = {
    _isSafe(bundle) match {
      case (result, reason, win) =>
        this.synchronized {
          stats = stats + (reason -> (stats.getOrElse(reason, 0) + 1))
        }
        (result, win)
    }
  }

  // reject OrderBundles, when there is another active order of the same exchange+tradepair still active
  def sameTradePairOrdersStillActive(bundle: OrderRequestBundle): Boolean = {
    val activeExchangeOrderPairs: Set[(String, TradePair)] = activeOrderBundles.values
      .flatMap(_.ordersRefs.map(o =>
        (o.exchange, o.tradePair))).toSet
    if (bundle.orderRequests.exists(o => activeExchangeOrderPairs.contains((o.exchange, o.tradePair)))) {
      if (log.isDebugEnabled())
        log.debug(s"rejecting new $bundle because another order of same exchange+tradepair is still active. Active trade pairs: $activeExchangeOrderPairs")
      else
        log.info(s"${Emoji.Disagree} rejecting new order bundle because same exchange+tradepair is still active")
      true
    } else false
  }

  private def _isSafe(bundle: OrderRequestBundle): (Boolean, SafetyGuardDecision, Option[Double]) = {

    def unsafe(d: SafetyGuardDecision): (Boolean, SafetyGuardDecision, Option[Double]) = (false, d, None)

    if (bundle.bill.sumUSDT <= 0) {
      log.warn(s"${Emoji.Disagree}  Got OrderBundle with negative balance: ${bundle.bill.sumUSDT}. I will not execute that one!")
      log.debug(s"$bundle")
      unsafe(NegativeBalance)
    } else if (!bundle.orderRequests.forall(tickerDataUpToDate)) {
      unsafe(TickerOutdated)
    } else if (bundle.bill.sumUSDT >= config.maximumReasonableWinPerOrderBundleUSDT) {
      log.warn(s"${Emoji.Disagree}  Got OrderBundle with unbelievable high estimated win of ${formatDecimal(bundle.bill.sumUSDT)} USDT. I will rather not execute that one - seem to be a bug!")
      log.debug(s"${Emoji.Disagree}  $bundle")
      unsafe(TooFantasticWin)
    } else if (!bundle.orderRequests.forall(orderLimitCloseToTicker))
      unsafe(OrderLimitFarAwayFromTicker)
    else if (sameTradePairOrdersStillActive(bundle)) {
      unsafe(SameTradePairOrderStillActive)
    } else {
      val r: (Boolean, Option[Double]) = totalTransactionsWinInRage(bundle)
      if (!r._1) unsafe(TotalTransactionUneconomic)
      else (true, Okay, r._2)
    }
  }
}

// TODO check if enough trading volume (or order book entries) are available to fulfill our trade plan