package org.purevalue.arbitrage.traderoom

import org.purevalue.arbitrage.traderoom.Asset.{BTC, ETH}
import org.purevalue.arbitrage.traderoom.exchange.Ticker
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.util.{IsoFiatCurrencies, WrongAssumption}


// Crypto asset / coin.
// It should NOT be created somewhere else - but registered via Asset.register().
// The way to get it is via Asset(officialSymbol)
class Asset(val officialSymbol: String,
            val name: Option[String],
            val isFiat: Boolean,
            val defaultFractionDigits: Int = 5,
            val sourceWeight: Int = 0) {

  private def canConvertIndirectly(targetAsset: Asset, intermediateAsset: Asset, conversionRateExists: TradePair => Boolean): Boolean = {
    canConvertDirectlyTo(intermediateAsset, conversionRateExists) &&
      intermediateAsset.canConvertDirectlyTo(targetAsset, conversionRateExists)
  }

  def canConvertDirectlyTo(targetAsset: Asset, conversionRateExists: TradePair => Boolean): Boolean = {
    this == targetAsset ||
      conversionRateExists(TradePair(this, targetAsset)) ||
      conversionRateExists(TradePair(targetAsset, this))
  }

  def canConvertTo(targetAsset: Asset, conversionRateExists: TradePair => Boolean): Boolean = {
    canConvertDirectlyTo(targetAsset, conversionRateExists) ||
      canConvertIndirectly(targetAsset, BTC, conversionRateExists)
  }

  def canConvertTo(targetAsset: Asset, ticker: Map[TradePair, Ticker]): Boolean = {
    canConvertTo(targetAsset, tp => ticker.contains(tp))
  }

  def canConvertTo(targetAsset: Asset, tradePairs: Set[TradePair]): Boolean = {
    canConvertTo(targetAsset, tp => tradePairs.contains(tp))
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Asset] &&
      this.officialSymbol == obj.asInstanceOf[Asset].officialSymbol
  }

  override def hashCode: Int = officialSymbol.hashCode

  override def toString: String = officialSymbol
}

object Asset {
  private val MaxSourceWeight = 10
  // some essential values we need everywhere
  private val PredefinedAssets: Seq[Asset] = Seq(
    new Asset("EUR", Some("Euro"), isFiat = true, 2, MaxSourceWeight),
    new Asset("USD", Some("U.S. Dollar"), isFiat = true, 2, MaxSourceWeight),
    new Asset("BTC", Some("Bitcoin"), isFiat = false, 8, MaxSourceWeight),
    new Asset("ETH", Some("Ethereum"), isFiat = false, 5, MaxSourceWeight),
    new Asset("USDT", Some("Tether"), isFiat = false, 2, MaxSourceWeight),
    new Asset("USDC", Some("USD Coin"), isFiat = false, 2, MaxSourceWeight)
  )

  // often used assets
  lazy val BTC: Asset = Asset("BTC")
  lazy val EUR: Asset = Asset("EUR")
  lazy val USD: Asset = Asset("USD")
  lazy val USDT: Asset = Asset("USDT")
  lazy val USDC: Asset = Asset("USDC")
  lazy val ETH: Asset = Asset("ETH")

  lazy val UsdEquivalentCoins: Set[Asset] = Set(USDT, USDC)

  // this is the reference to know exactly about which asset (or coin) we are talking (no matter at which exchange)
  private var allAssets: Map[String, Asset] = Map()

  def isKnown(officialSymbol: String): Boolean = allAssets.contains(officialSymbol)

  def register(a: Asset): Unit = register(a.officialSymbol, a.name, Some(a.isFiat), a.defaultFractionDigits, a.sourceWeight)

  def register(officialSymbol: String, name: String, isFiat: Boolean): Unit = register(officialSymbol, Some(name), Some(isFiat))

  def register(officialSymbol: String, name: Option[String], _isFiat: Option[Boolean], defaultFractionDigits: Int = 5, sourceWeight: Int = 0): Unit = {
    val FiatDefaultFractionDigits = 2
    val isFiat: Boolean = IsoFiatCurrencies.contains(officialSymbol) || _isFiat.contains(true)

    def mergeName(a: Option[String], b: Option[String]): Option[String] = Seq(a, b).flatten.headOption

    if (sourceWeight > MaxSourceWeight) throw new WrongAssumption("dynamic registered asset has a too high source weight")

    synchronized {
      allAssets =
        allAssets.get(officialSymbol) match {
          case None => allAssets + (
            IsoFiatCurrencies.get(officialSymbol) match {
              case Some(currencyName: String) => officialSymbol -> new Asset(officialSymbol, Some(currencyName), true, FiatDefaultFractionDigits, MaxSourceWeight)
              case None => officialSymbol -> new Asset(officialSymbol, name, isFiat, defaultFractionDigits, sourceWeight)
            })

          case Some(oldValue) if oldValue.sourceWeight < sourceWeight =>
            allAssets + (officialSymbol -> new Asset(officialSymbol, mergeName(name, oldValue.name), isFiat, defaultFractionDigits, sourceWeight))

          case Some(oldValue) if oldValue.name.isEmpty && name.isDefined =>
            allAssets + // merge name
              (oldValue.officialSymbol -> new Asset(oldValue.officialSymbol, name, oldValue.isFiat, oldValue.defaultFractionDigits, oldValue.sourceWeight))

          case _ => allAssets
        }
    }
  }

  def apply(officialSymbol: String): Asset = {
    val result: Option[Asset] =
      synchronized {
        allAssets.get(officialSymbol)
      }
    if (result.isEmpty) {
      throw new RuntimeException(s"Unknown asset with officialSymbol $officialSymbol")
    }
    result.get
  }

  PredefinedAssets.foreach(register)
}


// a universal usable trade-pair
case class TradePair(baseAsset: Asset, quoteAsset: Asset) extends Ordered[TradePair] {

  def compare(that: TradePair): Int = this.toString compare that.toString

  def reverse: TradePair = TradePair(quoteAsset, baseAsset)

  def involvedAssets: Set[Asset] = Set(baseAsset, quoteAsset)

  override def toString: String = s"${baseAsset.officialSymbol}:${quoteAsset.officialSymbol}"
}

case class FiatMoney(asset: Asset, amount: Double) {
  if (!asset.isFiat) throw new IllegalArgumentException(s"$asset is not Fiat Money")

  override def toString: String = s"""${formatDecimal(amount, 2)} ${asset.officialSymbol}"""
}

case class CryptoValue(asset: Asset, amount: Double) {
  if (asset.isFiat) throw new IllegalArgumentException(s"Fiat $asset isn't a crypto asset")

  override def toString: String = s"${formatDecimal(amount, asset.defaultFractionDigits)} ${asset.officialSymbol}"

  def canConvertTo(targetAsset: Asset, ticker: Map[TradePair, Ticker]): Boolean =
    this.asset.canConvertTo(targetAsset, ticker)

  def canConvertTo(targetAsset: Asset, tradePairs: Set[TradePair]): Boolean =
    this.asset.canConvertTo(targetAsset, tradePairs)

  def directConversion(to: Asset, findConversionRate: TradePair => Option[Double]): Option[CryptoValue] = {
    findConversionRate(TradePair(asset, to)) match { // try direct conversion first
      case Some(rate) => Some(CryptoValue(to, amount * rate))
      case None =>
        findConversionRate(TradePair(to, asset)) match { // try reverse trade pair
          case Some(rate) => Some(CryptoValue(to, amount / rate))
          case None => None
        }
    }
  }

  def convertTo(targetAsset: Asset, findConversionRate: TradePair => Option[Double]): CryptoValue = {

    def tryConvertVia(viaAsset: Asset): Option[CryptoValue] = {
      if ((this.asset != viaAsset && targetAsset != viaAsset) &&
        (findConversionRate(TradePair(this.asset, viaAsset)).isDefined || findConversionRate(TradePair(viaAsset, this.asset)).isDefined) &&
        (findConversionRate(TradePair(targetAsset, viaAsset)).isDefined || findConversionRate(TradePair(viaAsset, targetAsset)).isDefined)) {
        directConversion(viaAsset, findConversionRate).get
          .directConversion(targetAsset, findConversionRate)
      } else None
    }

    if (this.asset == targetAsset) this
    else {
      directConversion(targetAsset, findConversionRate) match {
        case Some(r) => r
        case None => tryConvertVia(BTC) match {
          case Some(r) => r
          case None => tryConvertVia(ETH) match {
            case Some(r) => r
            case None => throw new RuntimeException(s"No option available to convert $asset -> $targetAsset")
          }
        }
      }
    }
  }

  def convertTo(targetAsset: Asset, ticker: Map[TradePair, Ticker]): CryptoValue =
    convertTo(targetAsset, tp => ticker.get(tp).map(_.priceEstimate))
}
/**
 * CryptoValue on a specific exchange
 */
case class LocalCryptoValue(exchange: String, asset: Asset, amount: Double) {
  if (asset.isFiat) throw new IllegalArgumentException(s"Fiat $asset isn't a crypto asset")

  def cryptoValue: CryptoValue = CryptoValue(asset, amount)

  def negate: LocalCryptoValue = LocalCryptoValue(exchange, asset, -amount)

  def convertTo(targetAsset: Asset, findConversionRate: (String, TradePair) => Option[Double]): LocalCryptoValue = {
    val v = CryptoValue(asset, amount)
      .convertTo(targetAsset, tradepair => findConversionRate(exchange, tradepair))
    LocalCryptoValue(exchange, v.asset, v.amount)
  }

  override def toString: String = s"LocalCryptoValue($exchange: ${formatDecimal(amount, asset.defaultFractionDigits)} ${asset.officialSymbol})"
}
