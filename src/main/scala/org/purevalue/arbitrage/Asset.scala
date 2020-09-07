package org.purevalue.arbitrage

import org.purevalue.arbitrage.Asset.Bitcoin
import org.purevalue.arbitrage.TradeRoom.{ReferenceTicker, ReferenceTickerReadonly}
import org.purevalue.arbitrage.Utils.formatDecimal
import org.slf4j.LoggerFactory


// Crypto asset / coin.
// It should NOT be created somewhere else. The way to get it is via Asset(officialSymbol)
case class Asset(officialSymbol: String, name: String, visibleAmountFractionDigits: Int = 4) {
  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Asset] &&
      this.officialSymbol == obj.asInstanceOf[Asset].officialSymbol
  }

  override def hashCode: Int = officialSymbol.hashCode

  override def toString: String = s"$officialSymbol ($name)"
}
object Asset {
  // very often used assets
  val Bitcoin: Asset = Asset("BTC")
  val USDT: Asset = Asset("USDT")

  def apply(officialSymbol: String): Asset = {
    if (!StaticConfig.AllAssets.contains(officialSymbol)) {
      throw new RuntimeException(s"Unknown asset with officialSymbol $officialSymbol")
    }
    StaticConfig.AllAssets(officialSymbol)
  }
}


// a universal usable trade-pair
abstract class TradePair {

  def baseAsset: Asset
  def quoteAsset: Asset

  def reverse: TradePair = TradePair(quoteAsset, baseAsset)

  override def toString: String = s"${baseAsset.officialSymbol}:${quoteAsset.officialSymbol}"

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[TradePair] &&
      this.baseAsset == obj.asInstanceOf[TradePair].baseAsset &&
      this.quoteAsset == obj.asInstanceOf[TradePair].quoteAsset
  }

  override def hashCode(): Int = {
    val state = Seq(baseAsset, quoteAsset)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
object TradePair {
  def apply(b: Asset, q: Asset): TradePair = new TradePair {
    override val baseAsset: Asset = b
    override val quoteAsset: Asset = q
  }
}


case class CryptoValue(asset: Asset, amount: Double) {
  private val log = LoggerFactory.getLogger(classOf[CryptoValue])

  override def toString: String = s"${formatDecimal(amount, asset.visibleAmountFractionDigits)} ${asset.officialSymbol}"

  def convertTo(targetAsset: Asset, findConversionRate: TradePair => Option[Double]): Option[CryptoValue] = {
    if (this.asset == targetAsset)
      Some(this)
    else {
      // try direct conversion first
      findConversionRate(TradePair(this.asset, targetAsset)) match {
        case Some(rate) =>
          Some(CryptoValue(targetAsset, amount * rate))
        case None =>
          findConversionRate(TradePair(targetAsset, this.asset)) match { // try reverse ticker
            case Some(rate) =>
              Some(CryptoValue(targetAsset, amount / rate))
            case None => // try conversion via BTC as last option
              if ((this.asset != Bitcoin && targetAsset != Bitcoin)
                && findConversionRate(TradePair(this.asset, Bitcoin)).isDefined
                && findConversionRate(TradePair(targetAsset, Bitcoin)).isDefined) {
                this.convertTo(Bitcoin, findConversionRate).get.convertTo(targetAsset, findConversionRate)
              } else {
                log.debug(s"unable to convert $asset -> $targetAsset")
                None
              }
          }
      }
    }
  }

  def convertTo(targetAsset: Asset, referenceTicker: ReferenceTicker): Option[CryptoValue] =
    convertTo(targetAsset, referenceTicker.readonly)

  def convertTo(targetAsset: Asset, referenceTicker: ReferenceTickerReadonly): Option[CryptoValue] =
    convertTo(targetAsset, tp => referenceTicker.values.get(tp).map(_.currentPriceEstimate))

  def convertTo(targetAsset: Asset, localTicker: scala.collection.Map[TradePair, Ticker]): Option[CryptoValue] =
    convertTo(targetAsset, tp => localTicker.get(tp).map(_.priceEstimate))
}
/**
 * CryptoValue on a specific exchange
 */
case class LocalCryptoValue(exchange: String, asset: Asset, amount: Double) {
  def convertTo(targetAsset: Asset, findConversionRate: (String, TradePair) => Option[Double]): Option[LocalCryptoValue] =
    CryptoValue(asset, amount)
      .convertTo(targetAsset, tradepair => findConversionRate(exchange, tradepair))
      .map(e => LocalCryptoValue(exchange, e.asset, e.amount))

  def convertTo(targetAsset: Asset, tickers: Map[String, Map[TradePair, Ticker]]): Option[LocalCryptoValue] =
    CryptoValue(asset, amount)
      .convertTo(targetAsset, tickers(exchange))
      .map(e => LocalCryptoValue(exchange, e.asset, e.amount))
}
