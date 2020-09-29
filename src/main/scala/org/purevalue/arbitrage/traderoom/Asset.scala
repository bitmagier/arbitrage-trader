package org.purevalue.arbitrage.traderoom

import org.purevalue.arbitrage.StaticConfig
import org.purevalue.arbitrage.adapter.Ticker
import org.purevalue.arbitrage.traderoom.Asset.Bitcoin
import org.purevalue.arbitrage.traderoom.TradeRoom.TickersReadonly
import org.purevalue.arbitrage.util.Util.formatDecimal


// Crypto asset / coin.
// It should NOT be created somewhere else. The way to get it is via Asset(officialSymbol)
case class Asset(officialSymbol: String, name: String, defaultFractionDigits: Int = 5, isFiat: Boolean = false) {

  private def canConvertIndirectly(targetAsset: Asset, intermediateAsset: Asset, conversionRateExists: TradePair => Boolean): Boolean = {
    canConvertDirectlyTo(intermediateAsset, conversionRateExists) &&
      intermediateAsset.canConvertDirectlyTo(targetAsset, conversionRateExists)
  }

  private def canConvertDirectlyTo(targetAsset: Asset, conversionRateExists: TradePair => Boolean): Boolean = {
    this == targetAsset ||
      conversionRateExists.apply(TradePair(this, targetAsset)) ||
      conversionRateExists.apply(TradePair(targetAsset, this))
  }

  private def canConvertTo(targetAsset: Asset, conversionRateExists: TradePair => Boolean): Boolean = {
    canConvertDirectlyTo(targetAsset, conversionRateExists) ||
      canConvertIndirectly(targetAsset, Bitcoin, conversionRateExists)
  }

  def canConvertTo(targetAsset: Asset, ticker: collection.Map[TradePair, Ticker]): Boolean = {
    canConvertTo(targetAsset, tp => ticker.contains(tp))
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Asset] &&
      this.officialSymbol == obj.asInstanceOf[Asset].officialSymbol
  }

  override def hashCode: Int = officialSymbol.hashCode

  override def toString: String = s"$officialSymbol ($name)"
}

object Asset {
  // very often used assets
  val Euro: Asset = Asset("EUR")
  val USDollar: Asset = Asset("USD")
  val Bitcoin: Asset = Asset("BTC")

  val AssetUSDT: Asset = Asset("USDT")
  val AssetUSDC: Asset = Asset("USDC")

  def apply(officialSymbol: String): Asset = {
    if (!StaticConfig.AllAssets.contains(officialSymbol)) {
      throw new RuntimeException(s"Unknown asset with officialSymbol $officialSymbol")
    }
    StaticConfig.AllAssets(officialSymbol)
  }
}


// a universal usable trade-pair
abstract class TradePair extends Ordered[TradePair] {

  def baseAsset: Asset

  def quoteAsset: Asset

  def compare(that: TradePair): Int = this.toString compare that.toString

  def reverse: TradePair = TradePair(quoteAsset, baseAsset)

  def involvedAssets: Seq[Asset] = Seq(baseAsset, quoteAsset)

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

case class FiatMoney(asset: Asset, amount: Double) {
  if (!asset.isFiat) throw new IllegalArgumentException(s"$asset is not Fiat Money")

  override def toString: String = s"""${formatDecimal(amount, 2)} ${asset.officialSymbol}"""
}

case class CryptoValue(asset: Asset, amount: Double) {
  if (asset.isFiat) throw new IllegalArgumentException(s"Fiat $asset isn't a crypto asset")

  override def toString: String = s"${formatDecimal(amount, asset.defaultFractionDigits)} ${asset.officialSymbol}"

  def canConvertTo(targetAsset: Asset, ticker: collection.Map[TradePair, Ticker]): Boolean =
    this.asset.canConvertTo(targetAsset, ticker)

  def convertTo(targetAsset: Asset, findConversionRate: TradePair => Option[Double]): CryptoValue = {
    if (this.asset == targetAsset)
      this
    else {
      // try direct conversion first
      findConversionRate(TradePair(this.asset, targetAsset)) match {
        case Some(rate) =>
          CryptoValue(targetAsset, amount * rate)
        case None =>
          findConversionRate(TradePair(targetAsset, this.asset)) match { // try reverse ticker
            case Some(rate) =>
              CryptoValue(targetAsset, amount / rate)
            case None => // try conversion via BTC as last option
              if ((this.asset != Bitcoin && targetAsset != Bitcoin)
                && findConversionRate(TradePair(this.asset, Bitcoin)).isDefined
                && (findConversionRate(TradePair(targetAsset, Bitcoin)).isDefined || findConversionRate(TradePair(Bitcoin, targetAsset)).isDefined)) {
                this.convertTo(Bitcoin, findConversionRate).convertTo(targetAsset, findConversionRate)
              } else {
                throw new RuntimeException(s"No option available to convert $asset -> $targetAsset")
              }
          }
      }
    }
  }

  def convertTo(targetAsset: Asset, ticker: collection.Map[TradePair, Ticker]): CryptoValue =
    convertTo(targetAsset, tp => ticker.get(tp).map(_.priceEstimate))
}
/**
 * CryptoValue on a specific exchange
 */
case class LocalCryptoValue(exchange: String, asset: Asset, amount: Double) {
  if (asset.isFiat) throw new IllegalArgumentException(s"Fiat $asset isn't a crypto asset")

  def negate: LocalCryptoValue = LocalCryptoValue(exchange, asset, -amount)

  def convertTo(targetAsset: Asset, findConversionRate: (String, TradePair) => Option[Double]): LocalCryptoValue = {
    val v = CryptoValue(asset, amount)
      .convertTo(targetAsset, tradepair => findConversionRate(exchange, tradepair))
    LocalCryptoValue(exchange, v.asset, v.amount)
  }

  def convertTo(targetAsset: Asset, tickers: TickersReadonly): LocalCryptoValue = {
    val v = CryptoValue(asset, amount)
      .convertTo(targetAsset, tickers(exchange))
    LocalCryptoValue(exchange, v.asset, v.amount)
  }
}
