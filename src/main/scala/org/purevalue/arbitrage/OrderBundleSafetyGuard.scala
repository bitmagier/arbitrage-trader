package org.purevalue.arbitrage

import java.time.{Duration, Instant}

import org.purevalue.arbitrage.TradeRoom.OrderBundle
import org.purevalue.arbitrage.Utils.formatDecimal
import org.slf4j.LoggerFactory

import scala.collection._

case class OrderBundleSafetyGuard(config: OrderBundleSafetyGuardConfig,
                                  tickers: Map[String, concurrent.Map[TradePair, Ticker]],
                                  dataAge: Map[String, TPDataTimestamps]) {
  private val log = LoggerFactory.getLogger(classOf[OrderBundleSafetyGuard])

  private def orderLimitCloseToTicker(order: Order): Boolean = {
    val ticker = tickers(order.exchange)(order.tradePair)
    val bestOfferPrice: Double = if (order.direction == TradeSide.Buy) ticker.lowestAskPrice else ticker.highestBidPrice
    val diff = ((order.limit - bestOfferPrice) / bestOfferPrice).abs
    val valid = diff < config.maxOrderLimitTickerVariance
    if (!valid) {
      log.warn(s"${Emoji.Disagree} Got OrderBundle where a order limit is too far away ($diff) from ticker value " +
        s"(max variance=${formatDecimal(config.maxOrderLimitTickerVariance)})")
      log.debug(s"$order, $ticker")
    }
    valid
  }

  private def tickerDataUpToDate(o: Order): Boolean = {
    val age = Duration.between(dataAge(o.exchange).tickerTS, Instant.now)
    val r = age.compareTo(config.maxTickerAge) < 0
    if (!r) {
      log.warn(s"${Emoji.NoSupport} Sorry, can't let that order through, because we have an aged ticker (${age.toSeconds} s) for ${o.exchange} here.")
      log.debug(s"${Emoji.NoSupport} $o")
    }
    r
  }

  /**
   * Because the liquidity providing and returning to reserve liquidity transactions (done by the Liquidity Manager)
   * may ruin the plan to make a win out of the arbitrage trade bundle,
   * we simulate the following transactions:
   * - Providing Altcoins for the transaction(s) from a non-involved Reserve Asset on the same exchange
   * - Transforming the bought Altcoins back to that Reserve Asset on the same exchange
   * and then calculate the balance compared to the same 2 transactions done via the rate of the reference ticker
   */
  def balanceOfLiquidityTransformationCompensationTransactionsInUSDT(t: OrderBundle): Double = {
    // TODO implement
    0.0
  }

  def isSafe(t: OrderBundle): Boolean = {
    if (t.bill.sumUSDT <= 0) {
      log.warn(s"${Emoji.Disagree} Got OrderBundle with negative balance: ${t.bill.sumUSDT}. I will not execute that one!")
      log.debug(s"$t")
      false
    } else if (!t.orders.forall(tickerDataUpToDate)) {
      false
    } else if (t.bill.sumUSDT >= config.maximumReasonableWinPerOrderBundleUSDT) {
      log.warn(s"${Emoji.Disagree} Got OrderBundle with unbelievable high estimated win of ${formatDecimal(t.bill.sumUSDT)} USDT. I will rather not execute that one - seem to be a bug!")
      log.debug(s"${Emoji.Disagree} $t")
      false
    } else if (!t.orders.forall(orderLimitCloseToTicker)) {
      false
    } else if ((t.bill.sumUSDT + balanceOfLiquidityTransformationCompensationTransactionsInUSDT(t)) < config.minTotalGainInUSDT) {
      log.warn(s"${Emoji.LookingDown} Got interesting OrderBundle, but the costs of the necessary liquidity transformation transactions speak against it.")
      false
    } else {
      true
    }
  }

}
