package org.purevalue.arbitrage

import akka.actor.{ActorRef, Props}
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataInquirer, BinancePublicTPDataChannel}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexPublicDataInquirer, BitfinexPublicTPDataChannel}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.slf4j.LoggerFactory


case class ExchangeInitStuff(publicDataInquirerProps: Function1[ExchangeConfig, Props],
                             exchangePublicTPDataChannelProps: Function3[ExchangeConfig, TradePair, ActorRef, Props],
                             exchangeAccountDataChannelProps: Function2[ExchangeConfig, ActorRef, Props])

object StaticConfig {
  private val log = LoggerFactory.getLogger("StaticConfig")

  // all exchanges - used for init routine
  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      BinancePublicDataInquirer.props,
      BinancePublicTPDataChannel.props,
      BinanceAccountDataChannel.props
    ),
    "bitfinex" -> ExchangeInitStuff(
      BitfinexPublicDataInquirer.props,
      BitfinexPublicTPDataChannel.props,
      null // TODO
    )
  )

  // this is the reference to know exactly about which asset (or coin) we are talking (no matter at which exchange)
  val AllAssets: Map[String, Asset] = Seq(
    Asset("BTC", "Bitcoin", 8),
    Asset("ETH", "Ethereum", 6),
    Asset("XRP", "Ripple"),
    Asset("USDT", "Tether", 2),
    Asset("BCH", "Bitcoin Cash"),
    Asset("BSV", "Bitcoin SV"),
    Asset("LTC", "Litecoin"),
    Asset("ADA", "Cardano"),
    Asset("CRO", "Crypto.com Coin"),
    Asset("BNB", "Binance Coin"),
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
    Asset("EGLD", "Elrond"),
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
    Asset("BTG", "Bitcoin Gold"),
    Asset("BCD", "Bitcoin Diamond"),
    Asset("LSK", "Lisk"),
    Asset("WAVES", "Waves"),
    Asset("SXP", "Swipe"),
    Asset("BAND", "Band Protocol")
  ).map(a => (a.officialSymbol, a)).toMap

  log.info(s"Total available assets: ${AllAssets.keys}")
}

// TODO later: automatically initialize this list from a reliable network source (coinmarketcap.com etc) and limit it by a minimum market-cap of $100.000.000
