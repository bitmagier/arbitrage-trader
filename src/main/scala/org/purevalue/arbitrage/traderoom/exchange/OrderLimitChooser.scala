package org.purevalue.arbitrage.traderoom.exchange

import akka.event.Logging
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.traderoom.TradeSide

import scala.collection.{Iterator, Map}

class OrderLimitChooser(private val orderBook: Option[OrderBook], private val ticker: Ticker) {
    private val log = Logging(actorSystem.eventStream, getClass)
  /**
   * First we determine a theoretical ideal order limit (like when realityAdjustmentRate is 0.0) based on order book, if present,
   * to get our order through for the best available price. Ticker price is used instead, if no order book is present.
   *
   * In reality the order book changes between our snapshot timestamp and the timestamp when our order arrives at the exchange,
   * so the limit, we determine here, needs some adjustments before we have a real good order limit.
   * To adjust the (theoretical ideal) edge-limit to reality, we change the limit a bit to our disadvantage-side by the realityAdjustmentRate
   *
   * The result will be None, in case we have an order book available, but not enough entires in the order book to fulfil our order
   * (happened on bitfinex for TUSD:USDT)
   *
   * @param realityAdjustmentRate small relative rate to add/substract from the edge-order-limit to get our order through. 0.0002 is a good starting point
   */
  def determineRealisticOrderLimit(tradeSide: TradeSide, amountBaseAssetEstimate: Double, realityAdjustmentRate: Double): Option[Double] = {
    determineEdgeOrderLimit(tradeSide, amountBaseAssetEstimate)
      .map(edgeLimit => tradeSide match {
        case TradeSide.Buy => edgeLimit * (1.0 + realityAdjustmentRate)
        case TradeSide.Sell => edgeLimit * (1.0 - realityAdjustmentRate)
      })
  }

  /**
   * Determines the theoretical ideal order limit, based on order book, if present. Otherwise the ticker value is used.
   */
  def determineEdgeOrderLimit(tradeSide: TradeSide, amountBaseAssetEstimate: Double): Option[Double] = {
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

    def highestLevelToFulfillAmount(bids: Map[Double, Bid], amount: Double): Option[Double] = {
      val stackIterator = bids.values.map(e => (e.price, e.quantity)).toSeq.sortBy(_._1).reverseIterator // (price,quantity) sorted with highest price first
      val result = fillAmount(amount, stackIterator)
      if (result.isEmpty) {
        log.debug(s"Order book [${orderBook.map(_.exchange)} ${orderBook.map(_.tradePair)}] not filled enough to take our order: $tradeSide $amount. Bids: $bids")
      }
      result
    }

    def lowestLevelToFulfillAmount(asks: Map[Double, Ask], amount: Double): Option[Double] = {
      val stackIterator = asks.values.map(e => (e.price, e.quantity)).toSeq.sortBy(_._1).iterator // (price,quantity) sorted with lowest price first
      val result = fillAmount(amount, stackIterator)
      if (result.isEmpty) {
        log.debug(s"Order book [${orderBook.map(_.exchange)} ${orderBook.map(_.tradePair)}] not filled enough to take our order: $tradeSide $amount. Bids: $asks")
      }
      result
    }

    if (orderBook.isDefined) {
      tradeSide match {
        case TradeSide.Sell => highestLevelToFulfillAmount(orderBook.get.bids, amountBaseAssetEstimate)
        case TradeSide.Buy => lowestLevelToFulfillAmount(orderBook.get.asks, amountBaseAssetEstimate)
      }
    } else { // fallback for exchanges where we have no order book
      Some(ticker.priceEstimate)
    }
  }
}
