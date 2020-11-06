package org.purevalue.arbitrage.traderoom.exchange

import org.purevalue.arbitrage.traderoom.TradeSide
import org.slf4j.LoggerFactory

import scala.collection.{Iterator, Map}

class OrderLimitChooser(private val orderBook: Option[OrderBook], private val ticker: Ticker) {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * First we determine a theoretical ideal order limit (like when realityAdjustmentRate is 0.0) based on order book, if present,
   * to get our order through for the best available price. Ticker price is used instead, if no order book is present.
   *
   * In reality the order book changes between our snapshot timestamp, so we assume to have a higher quantity to trade (by multiplying the amount by orderbookBasedLimitQuantityOverbooking)
   * If the limit is calculated based on the ticker value, we adjust the (theoretical ideal) edge-limit to reality, by changing the limit a bit to our disadvantage-side by the tickerLimitRealityAdjustmentRate
   *
   * The result will be None, in case we have an order book available, but not enough entires in the order book to fulfil our order
   * (happened on bitfinex for TUSD:USDT). In this case we don't want to place an order there anyway.
   */
  def determineRealisticOrderLimit(tradeSide: TradeSide, amountBaseAssetEstimate: Double,
                                   orderbookBasedLimitQuantityOverbooking: Double, tickerLimitRealityAdjustmentRate: Double): Option[Double] = {
    //                                            Iterator (price , amount)  : limit
    def fillAmount(amount: Double, stackIterator: Iterator[(Double, Double)]): Option[Double] = {
      var filled: Double = 0.0
      var price: Option[Double] = None
      while (filled < amount && stackIterator.hasNext) {
        stackIterator.next() match {
          case (_price, _amount) =>
            price = Some(_price)
            filled = filled + _amount
        }
      }
      if (filled >= amount) price
      else None
    }

    /**
     * Determines the theoretical ideal order limit, based on order book, if present.
     */
    def highestLevelToFulfillAmount(bids: Map[Double, Bid], amount: Double): Option[Double] = {
      val stackIterator = bids.values.map(e => (e.price, e.quantity)).toSeq.sortBy(_._1).reverseIterator // (price,quantity) sorted with highest price first
      val result = fillAmount(amount, stackIterator)
      if (result.isEmpty) {
        log.debug(s"Order book [${orderBook.map(_.exchange)} ${orderBook.map(_.pair)}] not filled enough to take our order: $tradeSide $amount. Bids: $bids")
      }
      result
    }

    /**
     * Determines the theoretical ideal order limit, based on order book, if present.
     */
    def lowestLevelToFulfillAmount(asks: Map[Double, Ask], amount: Double): Option[Double] = {
      val stackIterator = asks.values.map(e => (e.price, e.quantity)).toSeq.sortBy(_._1).iterator // (price,quantity) sorted with lowest price first
      val result = fillAmount(amount, stackIterator)
      if (result.isEmpty) {
        log.debug(s"Order book [${orderBook.map(_.exchange)} ${orderBook.map(_.pair)}] not filled enough to take our order: $tradeSide $amount. Bids: $asks")
      }
      result
    }

    if (orderBook.isDefined) {
      tradeSide match {
        case TradeSide.Sell => highestLevelToFulfillAmount(orderBook.get.bids, amountBaseAssetEstimate * orderbookBasedLimitQuantityOverbooking)
        case TradeSide.Buy => lowestLevelToFulfillAmount(orderBook.get.asks, amountBaseAssetEstimate * orderbookBasedLimitQuantityOverbooking)
      }
    } else { // fallback for exchanges where we have no order book
      tradeSide match {
        case TradeSide.Sell => Some(ticker.priceEstimate * (1.0 - tickerLimitRealityAdjustmentRate))
        case TradeSide.Buy => Some(ticker.priceEstimate * (1.0 + tickerLimitRealityAdjustmentRate))
      }
    }
  }
}
