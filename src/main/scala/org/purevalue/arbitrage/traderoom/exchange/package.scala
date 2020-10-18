package org.purevalue.arbitrage.traderoom

import java.time.{Duration, Instant}

import akka.actor.{ActorRef, Props}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.OrderBookTooFlatException
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{Config, ExchangeConfig, GlobalConfig}

package object exchange {
  type ExchangePublicDataInquirerInit = Function2[GlobalConfig, ExchangeConfig, Props]
  type ExchangePublicDataChannelInit = Function5[GlobalConfig, ExchangeConfig, Set[TradePair], ActorRef, ActorRef, Props]
  type ExchangeAccountDataChannelInit = Function4[Config, ExchangeConfig, ActorRef, ActorRef, Props]

  trait ExchangePublicStreamData extends Retryable

  case class Heartbeat(ts: Instant) extends ExchangePublicStreamData
  case class Ticker(exchange: String,
                    tradePair: TradePair,
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
                       tradePair: TradePair,
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
  }

  case class OrderBookUpdate(exchange: String,
                             tradePair: TradePair,
                             bidUpdates: Seq[Bid], // quantity == 0.0 means: remove position from our OrderBook
                             askUpdates: Seq[Ask]) extends ExchangePublicStreamData


  trait Retryable {
    val MaxApplyDelay: Duration = Duration.ofMillis(200)
    var applyDeadline: Option[Instant] = None // when this dataset can be applied latest before timing out
  }

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
      s"Wallet [$exchange]: Liquid crypto aggregated total: $liquidity. " +
        s"""Detailed: [${liquidCryptoValues(aggregateAsset, ticker).mkString(", ")}]""" +
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

    // "liquid crypto values" are our wallet value of crypt assets, which are available for trading and converting-calculations
    def liquidCryptoValues(aggregateAsset: Asset, ticker: Map[TradePair, Ticker]): Iterable[CryptoValue] =
      balance
        .filterNot(_._1.isFiat)
        .filterNot(b => doNotTouchTheseAssets.contains(b._1))
        .map(b => CryptoValue(b._1, b._2.amountAvailable))
        .filter(_.canConvertTo(aggregateAsset, ticker))

    def liquidCryptoValueSum(aggregateAsset: Asset, ticker: Map[TradePair, Ticker]): CryptoValue = {
      liquidCryptoValues(aggregateAsset, ticker)
        .map(_.convertTo(aggregateAsset, ticker))
        .foldLeft(CryptoValue(aggregateAsset, 0.0))((a, x) => CryptoValue(a.asset, a.amount + x.amount))
    }
  }

  case class SimulatedAccountData(dataset: ExchangeAccountStreamData)

  case class CompleteWalletUpdate(balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
  case class WalletAssetUpdate(balance: Map[Asset, Balance]) extends ExchangeAccountStreamData
  case class WalletBalanceUpdate(asset: Asset, amountDelta: Double) extends ExchangeAccountStreamData


  def determineRealisticLimit(pair: TradePair,
                              side: TradeSide,
                              quantity: Double,
                              realityAdjustmentRate: Double,
                              ticker: Map[TradePair, Ticker],
                              orderBook: Map[TradePair, OrderBook]): Double = {
    new OrderLimitChooser(orderBook.get(pair), ticker(pair))
      .determineRealisticOrderLimit(side, quantity, realityAdjustmentRate) match {
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
                              realityAdjustmentRate: Double,
                              ticker: Map[TradePair, Ticker],
                              referenceTicker: Map[TradePair, Ticker],
                              orderBook: Map[TradePair, OrderBook]
                             ): Double = {
    val localLimit = determineRealisticLimit(pair, side, quantity, realityAdjustmentRate, ticker, orderBook)
    localExchangeRateRating(pair, side, localLimit, referenceTicker)
  }
}