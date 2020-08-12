package org.purevalue.arbitrage

import java.time.{Duration, Instant}

import org.purevalue.arbitrage.TradeRoom.OrderBundle
import org.purevalue.arbitrage.Utils.formatDecimal
import org.slf4j.LoggerFactory

import scala.collection._

case class OrderBundleValidityGuard(config: OrderBundleValidityGuardConfig,
                                    tickers: Map[String, concurrent.Map[TradePair, Ticker]],
                                    dataAge: Map[String, TPDataTimestamps]) {
  private val log = LoggerFactory.getLogger(classOf[OrderBundleValidityGuard])

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
      log.warn(s"${Emoji.NoSupport} Sorry, can't let that order through, because we don't have an aged ticker (${age.toSeconds} s) for ${o.exchange} here.")
      log.debug(s"${Emoji.NoSupport} $o")
    }
    r
  }

  def isValid(t: OrderBundle): Boolean = {
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
    } else {
      true
    }
  }

}
