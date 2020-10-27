package org.purevalue.arbitrage.adapter.binance

/**
 * @see https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
 */
object BinanceErrorCodes {
  val NoSuchOrder: Int = -2013 // order does not exist
  val CancelRejected: Int = -2011
  val NewOrderRejected: Int = -2010
}
