package org.purevalue.arbitrage

import java.time.{Duration, Instant}

import org.purevalue.arbitrage.TradeRoom.OrderBundle
import org.purevalue.arbitrage.Utils.formatDecimal
import org.slf4j.LoggerFactory

case class OrderBundleSafetyGuard(config: OrderBundleSafetyGuardConfig,
                                  tickers: scala.collection.Map[String, scala.collection.concurrent.Map[TradePair, Ticker]],
                                  extendedTicker: scala.collection.Map[String, scala.collection.Map[TradePair, ExtendedTicker]],
                                  dataAge: scala.collection.Map[String, TPDataTimestamps]) {
  private val log = LoggerFactory.getLogger(classOf[OrderBundleSafetyGuard])

  private def orderLimitCloseToTicker(order: Order): Boolean = {
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

  private def tickerDataUpToDate(o: Order): Boolean = {
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
  def balanceOfLiquidityTransformationCompensationTransactionsInUSDT(t: OrderBundle): Option[Double] = {
    val allReserveAssets: List[Asset] = AppConfig.liquidityManager.reserveAssets
    val involvedAssets: Set[Asset] = t.orders.flatMap(e => Seq(e.tradePair.baseAsset, e.tradePair.quoteAsset)).toSet
    val uninvolvedReserveAssets: List[Asset] = allReserveAssets.filterNot(involvedAssets.contains)

    val toProvide: Iterable[LocalCryptoValue] = t.orders.map(_.calcOutgoingLiquidity).filterNot(allReserveAssets.contains)
    val toConvertBack: Iterable[LocalCryptoValue] = t.orders.map(_.calcIncomingLiquidity).filterNot(allReserveAssets.contains)

    def findUsableReserveAsset(exchange: String, coin: Asset, possibleReserveAssets: List[Asset]): Option[Asset] = {
      possibleReserveAssets.find(r => tickers(exchange).contains(TradePair.of(coin, r)))
    }

    val unableToProvideConversionForCoin: Option[LocalCryptoValue] = {
      (toProvide ++ toConvertBack).find(v => findUsableReserveAsset(v.exchange, v.asset, uninvolvedReserveAssets).isEmpty)
    }
    if (unableToProvideConversionForCoin.isDefined) {
      log.warn(s"${Emoji.EyeRoll}  Sorry, regarding no suitable reserve asset found to support reserve liquidity conversion from/to ${unableToProvideConversionForCoin.get.asset} on ${unableToProvideConversionForCoin.get.exchange}. Concerns $t")
      return None
    }

    val transactions =
      toProvide.map(e => {
        val tradePair = TradePair.of(e.asset, findUsableReserveAsset(e.exchange, e.asset, uninvolvedReserveAssets).get)
        Order(null, null,
          e.exchange,
          tradePair,
          TradeSide.Buy,
          Fee(e.exchange, AppConfig.exchange(e.exchange).makerFee, AppConfig.exchange(e.exchange).takerFee), // TODO take real values
          e.amount,
          tickers(e.exchange)(tradePair).priceEstimate
        )
      }) ++ toConvertBack.map(e => {
          val tradePair = TradePair.of(e.asset, findUsableReserveAsset(e.exchange, e.asset, uninvolvedReserveAssets).get)
          Order(null, null,
            e.exchange,
            tradePair,
            TradeSide.Sell,
            Fee(e.exchange, AppConfig.exchange(e.exchange).makerFee, AppConfig.exchange(e.exchange).takerFee), // TODO take real values
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
        tp => TradeRoom.findReferenceTicker(tp, extendedTicker).map(_.weightedAveragePrice)))
  }


  def totalTransactionCostsInRage(t: OrderBundle): Boolean = {
    val b: Option[Double] = balanceOfLiquidityTransformationCompensationTransactionsInUSDT(t)
    if (b.isEmpty) return false
    if ((t.bill.sumUSDT + b.get) < config.minTotalGainInUSDT) {
      log.debug(s"${Emoji.LookingDown}  Got interesting $t, but the sum of costs (${formatDecimal(b.get)} USDT) of the necessary " +
        s"liquidity transformation transactions makes the whole thing uneconomic (total gain: ${formatDecimal(t.bill.sumUSDT + b.get)} USDT = lower than threshold ${config.minTotalGainInUSDT} USDT).")
      false
    } else true
  }

  def isSafe(t: OrderBundle): Boolean = {
    if (t.bill.sumUSDT <= 0) {
      log.warn(s"${Emoji.Disagree}  Got OrderBundle with negative balance: ${t.bill.sumUSDT}. I will not execute that one!")
      log.debug(s"$t")
      false
    } else if (!t.orders.forall(tickerDataUpToDate)) {
      false
    } else if (t.bill.sumUSDT >= config.maximumReasonableWinPerOrderBundleUSDT) {
      log.warn(s"${Emoji.Disagree}  Got OrderBundle with unbelievable high estimated win of ${formatDecimal(t.bill.sumUSDT)} USDT. I will rather not execute that one - seem to be a bug!")
      log.debug(s"${Emoji.Disagree}  $t")
      false
    } else {
      t.orders.forall(orderLimitCloseToTicker) &&
        totalTransactionCostsInRage(t)
    }
  }
}
