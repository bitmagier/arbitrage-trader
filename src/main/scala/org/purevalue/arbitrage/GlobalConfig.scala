package org.purevalue.arbitrage

import akka.actor.{ActorRef, Props}
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataInquirer, BinancePublicTPDataChannel}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexPublicDataInquirer, BitfinexPublicTPDataChannel}
import org.slf4j.LoggerFactory

// Crypto asset / coin.
// It should NOT be created somewhere else. The way to get it is via Asset(officialSymbol)
case class Asset(officialSymbol: String, name: String) {
  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Asset] &&
      this.officialSymbol == obj.asInstanceOf[Asset].officialSymbol
  }

  override def hashCode(): Int = officialSymbol.hashCode
}
object Asset {
  // very often used assets
  val Bitcoin:Asset = Asset("BTC")
  val USDT:Asset = Asset("USDT")

  def apply(officialSymbol: String): Asset = {
    if (!GlobalConfig.AllAssets.contains(officialSymbol)) {
      throw new RuntimeException(s"Unknown asset with officialSymbol $officialSymbol")
    }
    GlobalConfig.AllAssets(officialSymbol)
  }
}


// a universal usable trade-pair
abstract class TradePair {
  val baseAsset: Asset
  val quoteAsset: Asset

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
  def of(b: Asset, q: Asset): TradePair = new TradePair {
    override val baseAsset: Asset = b
    override val quoteAsset: Asset = q
  }
}

case class ExchangePublicTPDataChannelPropsParams(tp: TradePair, exchangePublicDataInquirer: ActorRef, tpDataManager: ActorRef)
case class ExchangeInitStuff(publicDataInquirerProps: Function0[Props],
                             exchangePublicTPDataChannelProps: Function1[ExchangePublicTPDataChannelPropsParams, Props],
                             exchangeAccountDataChannelProps: () => Props)

object GlobalConfig {
  private val log = LoggerFactory.getLogger("GlobalConfig")

  // all exchanges - used for init routine
  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      () => BinancePublicDataInquirer.props(Config.exchange("binance")),
      (p: ExchangePublicTPDataChannelPropsParams) =>
        BinancePublicTPDataChannel.props(Config.exchange("binance"), p.tp, p.exchangePublicDataInquirer),
      () => BinanceAccountDataChannel.props(Config.exchange("binance"))
    ),
    "bitfinex" -> ExchangeInitStuff(
      () => BitfinexPublicDataInquirer.props(Config.exchange("bitfinex")),
      (p: ExchangePublicTPDataChannelPropsParams) =>
        BitfinexPublicTPDataChannel.props(Config.exchange("bitfinex"), p.tp, p.exchangePublicDataInquirer),
      null // TODO
    )
  )

  // this is the reference to know exactly about which asset (or coin) we are talking (no matter at which exchange)
  val AllAssets: Map[String, Asset] = Seq(
    Asset("BTC", "Bitcoin"),
    Asset("ETH", "Ethereum"),
    Asset("XRP", "Ripple"),
    Asset("USDT", "Tether"),
    Asset("BCH", "Bitcoin Cash"),
    Asset("BSV", "Bitcoin SV"),
    Asset("LTC", "Litecoin"),
    Asset("ADA", "Cardano"),
    Asset("CRO", "Crypto.com Coin"),
    //    Asset("BNB", "Binance Coin"),
    Asset("EOS", "EOS"),
    Asset("LINK", "Chainlink"),
    Asset("XTZ", "Tezos"),
    Asset("XLM", "Stellar"),
    Asset("XMR", "Monero"),
    Asset("TRX", "TRON"),
    Asset("USDC", "USD Coin"),
    Asset("HT", "Huobi Token"),
    Asset("VET", "VeChain"),
    Asset("NEO", "Neo"),
    Asset("ETC", "Ethereum Classic"),
    Asset("MIOTA", "IOTA"),
    Asset("DASH", "Dash"),
    Asset("ZEC", "Zcash"),
    Asset("ATOM", "Cosmos"),
    Asset("MKR", "Maker"),
    Asset("ONT", "Ontology"),
    Asset("XEM", "NEM"),
    Asset("DOGE", "Dogecoin"),
    Asset("HEDG", "HedgeTrade"),
    Asset("LEND", "Aave"),
    Asset("AMPL", "Ampleforth"),
    Asset("BAT", "Basic Attention Token"),
    Asset("OKB", "OKB"),
//    Asset("DAI", "Dai"),
    Asset("COMP", "Compound"),
    Asset("SNX", "Synthetix Network Token"),
  //  Asset("DGB", "DigiByte"),
    Asset("KNC", "Kyber Network"),
    Asset("ERD", "Elrond"),
    Asset("ZRX", "0x"),
    Asset("THETA", "THETA"),
    Asset("BTT", "BitTorrent"),
    Asset("ALGO", "Algorand"),
    Asset("HYN", "Hyperion"),
    Asset("QTUM", "Qtum"),
    Asset("PAX", "Paxos Standard"),
    Asset("OMG", "OMG Network"),
    Asset("REP", "Augur"),
    Asset("HBAR", "Hedera Hashgraph"),
    Asset("TUSD", "TrueUSD"),
    Asset("ICX", "ICON"),
    Asset("ZIL", "Zilliqa"),
    Asset("DCR", "Decred"),
    // Asset("BTG", "Bitcoin Gold"),
    Asset("BCD", "Bitcoin Diamond"),
    Asset("LSK", "Lisk"),
    Asset("WAVES", "Waves"),
    Asset("SXP", "Swipe")
  ).map(a => (a.officialSymbol, a)).toMap

  log.info(s"Total available assets: ${AllAssets.keys}")
}

// TODO later: automatically initialize this list from a reliable network source (coinmarketcap.com etc) and limit it by a minimum market-cap of $100.000.000
