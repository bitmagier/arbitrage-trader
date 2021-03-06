package org.purevalue.arbitrage.traderoom

import java.time.{Duration, Instant}

import akka.actor.typed.{ActorRef, Behavior}
import org.purevalue.arbitrage.adapter.{AccountDataChannel, PublicDataChannel, PublicDataInquirer}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.OrderBookTooFlatException
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{Config, ExchangeConfig, GlobalConfig}

package object exchange {
  type ExchangePublicDataInquirerInit = Function2[GlobalConfig, ExchangeConfig, Behavior[PublicDataInquirer.Command]]
  type ExchangePublicDataChannelInit = Function5[GlobalConfig, ExchangeConfig, Set[TradePair], ActorRef[Exchange.Message], ActorRef[PublicDataInquirer.Command], Behavior[PublicDataChannel.Event]]
  type ExchangeAccountDataChannelInit = Function4[Config, ExchangeConfig, ActorRef[Exchange.Message], ActorRef[PublicDataInquirer.Command], Behavior[AccountDataChannel.Command]]

  trait ExchangePublicStreamData extends Retryable

  case class Heartbeat(ts: Instant) extends ExchangePublicStreamData

  case class DataAge(heartbeatTS: Option[Instant],
                     tickerTS: Option[Instant],
                     orderBookTS: Option[Instant]) {
    def withHeartBeatTS(ts: Instant): DataAge = DataAge(Some(ts), tickerTS, orderBookTS)

    def withTickerTS(ts: Instant): DataAge = DataAge(heartbeatTS, Some(ts), orderBookTS)

    def withOrderBookTS(ts: Instant): DataAge = DataAge(heartbeatTS, tickerTS, Some(ts))

    def latest: Instant = (heartbeatTS.toSeq ++ tickerTS.toSeq ++ orderBookTS.toSeq).max
  }

  case class Ticker(exchange: String,
                    pair: TradePair,
                    highestBidPrice: Double,
                    highestBidQuantity: Option[Double],
                    lowestAskPrice: Double,
                    lowestAskQuantity: Option[Double],
                    lastPrice: Option[Double]) extends ExchangePublicStreamData {
    def priceEstimate: Double = lastPrice match {
      case Some(last) => (highestBidPrice + lowestAskPrice + last) / 3
      case None => (highestBidPrice + lowestAskPrice) / 2
    }
  }

  /**
   * A bid is an offer to buy an asset.
   * (likely aggregated) bid position(s) for a price level
   */
  case class Bid(price: Double, quantity: Double) {
    override def toString: String = s"Bid(price=${formatDecimal(price)}, amount=${formatDecimal(quantity)})"
  }

  /**
   * An ask is an offer to sell an asset.
   * (likely aggregated) ask position(s) for a price level
   */
  case class Ask(price: Double, quantity: Double) {
    override def toString: String = s"Ask(price=${formatDecimal(price)}, amount=${formatDecimal(quantity)})"
  }

  case class OrderBook(exchange: String,
                       pair: TradePair,
                       bids: Map[Double, Bid], // price-level -> bid
                       asks: Map[Double, Ask] // price-level -> ask
                      ) extends ExchangePublicStreamData {
    def toCondensedString: String = {
      val bestBid = highestBid
      val bestAsk = lowestAsk
      s"${bids.keySet.size} Bids (highest price: ${formatDecimal(bestBid.price)}, quantity: ${bestBid.quantity}) " +
        s"${asks.keySet.size} Asks(lowest price: ${formatDecimal(bestAsk.price)}, quantity: ${bestAsk.quantity})"
    }

    def highestBid: Bid = bids(bids.keySet.max)

    def lowestAsk: Ask = asks(asks.keySet.min)

    /**
     * @return sum of bid quantity (want to buy) in the price range from current-price till price level at (1-rate) * current-price
     */
    def bidDepthAround(rate: Double): CryptoValue = {
      val limit = highestBid.price * (1.0 - rate)
      val quantity = bids.filter(_._1 >= limit).values.foldLeft(0.0)((sum, e) => sum + e.quantity)
      CryptoValue(pair.baseAsset, quantity)
    }

    /**
     * @return summed of ask auantity (want to sell) in the price range from current-price till price level at (1+rate) * current-price
     */
    def askDepthAround(rate: Double): CryptoValue = {
      val limit = lowestAsk.price * (1.0 + rate)
      val quantity = asks.filter(_._1 <= limit).values.foldLeft(0.0)((sum, e) => sum + e.quantity)
      CryptoValue(pair.baseAsset, quantity)
    }
  }

  case class OrderBookUpdate(exchange: String,
                             pair: TradePair,
                             bidUpdates: Seq[Bid], // quantity == 0.0 means: remove position from our OrderBook
                             askUpdates: Seq[Ask]) extends ExchangePublicStreamData


  trait Retryable {
    val MaxApplyDelay: Duration = Duration.ofMillis(200)
    var applyDeadline: Option[Instant] = None // when this dataset can be applied latest before timing out
  }

  case class Stats24h(exchange: String,
                      pair: TradePair,
                      averagePrice: Double,
                      volumeBaseAsset: Double) extends ExchangePublicStreamData

  trait ExchangeAccountStreamData extends Retryable

  case class Balance(asset: Asset, amountAvailable: Double, amountLocked: Double) {
    override def toString: String = s"Balance(${asset.officialSymbol}: " +
      s"available:${formatDecimal(amountAvailable, asset.defaultFractionDigits)}, " +
      s"locked: ${formatDecimal(amountLocked, asset.defaultFractionDigits)})"
  }

  case class Wallet(exchange: String, doNotTouchTheseAssets: Set[Asset], balance: Map[Asset, Balance]) {

    def withBalance(balance: Map[Asset, Balance]): Wallet = Wallet(exchange, doNotTouchTheseAssets, balance)

    def toOverviewString(aggregateAsset: Asset, ticker: Map[TradePair, Ticker]): String = {
      val liquidity = this.liquidCryptoValueSum(aggregateAsset, ticker)
      val inconvertible = this.inconvertibleCryptoValues(aggregateAsset, ticker)
      s"Wallet [$exchange]: Liquid crypto total sum: $liquidity. " +
        s"""In detail: [${liquidCryptoValues(aggregateAsset, ticker).mkString(", ")}]""" +
        (if (inconvertible.nonEmpty) s"""; Inconvertible to ${aggregateAsset.officialSymbol}: [${inconvertible.mkString(", ")}]""" else "") +
        (if (this.fiatMoney.nonEmpty) s"""; Fiat Money: [${this.fiatMoney.mkString(", ")}]""" else "") +
        (if (this.notTouchValues.nonEmpty) s""", Not-touching: [${this.notTouchValues.mkString(", ")}]""" else "")
    }

    override def toString: String = s"""Wallet($exchange, ${balance.mkString(",")})"""

    def inconvertibleCryptoValues(aggregateAsset: Asset, ticker: Map[TradePair, Ticker]): Seq[CryptoValue] =
      balance
        .filterNot(_._1.isFiat)
        .map(e => CryptoValue(e._1, e._2.amountAvailable))
        .filter(e => !e.canConvertTo(aggregateAsset, ticker))
        .toSeq
        .sortBy(_.asset.officialSymbol)

    def notTouchValues: Seq[CryptoValue] =
      balance
        .filter(e => doNotTouchTheseAssets.contains(e._1))
        .map(e => CryptoValue(e._1, e._2.amountAvailable))
        .toSeq
        .sortBy(_.asset.officialSymbol)

    def fiatMoney: Seq[FiatMoney] =
      balance
        .filter(_._1.isFiat)
        .map(e => FiatMoney(e._1, e._2.amountAvailable))
        .toSeq
        .sortBy(_.asset.officialSymbol)

    def availableCryptoValues(): Iterable[CryptoValue] = {
      balance
        .filterNot(_._1.isFiat)
        .filterNot(e => doNotTouchTheseAssets.contains(e._1))
        .map(e => CryptoValue(e._1, e._2.amountAvailable))
    }

    // "liquid crypto values" are our wallet value of crypt assets, which are available for trading and converting-calculations
    def liquidCryptoValues(aggregateAsset: Asset, ticker: Map[TradePair, Ticker]): Seq[CryptoValue] =
      balance
        .filterNot(_._1.isFiat)
        .filterNot(b => doNotTouchTheseAssets.contains(b._1))
        .map(b => CryptoValue(b._1, b._2.amountAvailable))
        .filter(_.canConvertTo(aggregateAsset, ticker))
        .toSeq
        .sortBy(_.convertTo(aggregateAsset, ticker).amount)
        .reverse


    def liquidCryptoValueSum(aggregateAsset: Asset, ticker: Map[TradePair, Ticker]): CryptoValue = {
      liquidCryptoValues(aggregateAsset, ticker)
        .map(_.convertTo(aggregateAsset, ticker))
        .foldLeft(CryptoValue(aggregateAsset, 0.0))((a, x) => CryptoValue(a.asset, a.amount + x.amount))
    }
  }

  case class CompleteWalletUpdate(balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
  case class WalletAssetUpdate(balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
  case class WalletBalanceUpdate(asset: Asset, amountDelta: Double) extends ExchangeAccountStreamData


  def determineRealisticLimit(pair: TradePair,
                              side: TradeSide,
                              quantity: Double,
                              orderbookBasedLimitQuantityOverbooking: Double,
                              tickerLimitRealityAdjustmentRate: Double,
                              ticker: Map[TradePair, Ticker],
                              orderBook: Map[TradePair, OrderBook]): Double = {
    new OrderLimitChooser(orderBook.get(pair), ticker(pair))
      .determineRealisticOrderLimit(side, quantity, orderbookBasedLimitQuantityOverbooking, tickerLimitRealityAdjustmentRate) match {
      case Some(limit) => limit
      case None => throw new OrderBookTooFlatException(pair, side)
    }
  }

  /**
   * Gives back a rating for trading a amount on that tradepair on the local exchange - compared to reference ticker
   * Ratings can be interpreted as a percentage of the local limit needed to fulfill a local trade compared to the reference ticker.
   * A result rate > 0 indicates a positive rating - being better than the reference ticker, a negative rate the opposite
   */
  def localExchangeRateRating(pair: TradePair,
                              side: TradeSide,
                              localLimit: Double,
                              referenceTicker: Map[TradePair, Ticker]): Double = {
    referenceTicker.get(pair).map(_.priceEstimate) match {
      case Some(referencePrice) => side match {
        case TradeSide.Buy => 1.0 - localLimit / referencePrice // 1 - x/R
        case TradeSide.Sell => localLimit / referencePrice - 1.0 // x/R - 1
      }
      case None => 0.0 // we can not judge it, because no reference ticker available
    }
  }

  def localExchangeRateRating(pair: TradePair,
                              side: TradeSide,
                              quantity: Double,
                              orderbookBasedLimitQuantityOverbooking: Double,
                              tickerLimitRealityAdjustmentRate: Double,
                              ticker: Map[TradePair, Ticker],
                              referenceTicker: Map[TradePair, Ticker],
                              orderBook: Map[TradePair, OrderBook]
                             ): Double = {
    val localLimit = determineRealisticLimit(pair, side, quantity, orderbookBasedLimitQuantityOverbooking, tickerLimitRealityAdjustmentRate, ticker, orderBook)
    localExchangeRateRating(pair, side, localLimit, referenceTicker)
  }
}