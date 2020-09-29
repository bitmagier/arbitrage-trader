package org.purevalue.arbitrage

import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataChannel, BinancePublicDataInquirer}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexAccountDataChannel, BitfinexPublicDataChannel, BitfinexPublicDataInquirer}
import org.purevalue.arbitrage.adapter.coinbase.{CoinbaseAccountDataChannel, CoinbasePublicDataChannel, CoinbasePublicDataInquirer}
import org.purevalue.arbitrage.traderoom.Asset
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{ExchangeAccountDataChannelInit, ExchangePublicDataChannelInit, ExchangePublicDataInquirerInit}
import org.slf4j.LoggerFactory

case class ExchangeInitStuff(exchangePublicDataInquirerProps: ExchangePublicDataInquirerInit,
                             exchangePublicDataChannelProps: ExchangePublicDataChannelInit,
                             exchangeAccountDataChannelProps: ExchangeAccountDataChannelInit)

object StaticConfig {
  private val log = LoggerFactory.getLogger("StaticConfig")

  // all exchanges - used for init routine
  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      BinancePublicDataInquirer.props,
      BinancePublicDataChannel.props,
      BinanceAccountDataChannel.props
    ),
    "bitfinex" -> ExchangeInitStuff(
      BitfinexPublicDataInquirer.props,
      BitfinexPublicDataChannel.props,
      BitfinexAccountDataChannel.props
    ),
    "coinbase" -> ExchangeInitStuff(
      CoinbasePublicDataInquirer.props,
      CoinbasePublicDataChannel.props,
      CoinbaseAccountDataChannel.props
    )
  )

  // this is the reference to know exactly about which asset (or coin) we are talking (no matter at which exchange)
  val AllAssets: Map[String, Asset] = Seq(
    Asset("EUR", "Euro", 2, isFiat = true),
    Asset("USD", "U.S. Dollar", 2, isFiat = true),
    Asset("BTC", "Bitcoin", 8),
    Asset("ETH", "Ethereum", 6),
    Asset("USDT", "Tether", 2),
    Asset("XRP", "Ripple"),
    Asset("EGLD", "Elrond"), //
    Asset("BCH", "Bitcoin Cash"),
    Asset("BNB", "Binance Coin"),
    Asset("LINK", "Chainlink"),
    Asset("ADA", "Cardano"),
    Asset("BSV", "Bitcoin SV"),
    Asset("LTC", "Litecoin"),
    Asset("EOS", "EOS"),
    Asset("USDC", "USD Coin"),
    Asset("TRX", "TRON"),
    Asset("XTZ", "Tezos"),
    Asset("XMR", "Monero"),
    Asset("XLM", "Stellar Lumens"),
    Asset("NEO", "NEO"),
    Asset("LEO", " Unus Sed Leo"),
    Asset("XEM", "NEM"),
    Asset("ATOM", "Cosmos"),
    Asset("SNX", "Synthetix Network Token"),
    Asset("CRO", "Crypto.com Chain"),
    Asset("ALGO", "Algorand"),
    Asset("VET", "VeChain"),
    Asset("MIOTA", "IOTA"),
    Asset("LEND", "ETHLend"),
    Asset("DASH", "Dash"),
    Asset("ETC", "Ethereum Classic"),
    Asset("CEL", "Celsius"),
    Asset("THETA", "THETA"),
    Asset("OMG", "OmiseGO"),
    Asset("ZEC", "Zcash"),
    Asset("MKR", "Maker"),
    Asset("TUSD", "TrueUSD"),
    Asset("ONT", "Ontology"),
    Asset("UNI**", "Uniswap"),
    Asset("HEDG", "HedgeTrade"),
    Asset("BAT", "Basic Attention Token"),
    Asset("DGB", "DigiByte"),
    Asset("DOGE", "Dogecoin"),
    Asset("ZRX", "0x"),
    Asset("PAX", "Paxos Standard Token"),
    Asset("LRC", "Loopring"),
    Asset("QTUM", "Qtum"),
    Asset("WAVES", "Waves"),
    Asset("NUT", "Native Utility Token"),
    Asset("ICX", "ICON"),
    Asset("DAI", "Dai"),
    Asset("HT", "Huobi Token"),
    Asset("REN", "REN"),
    Asset("HBAR", "Hedera Hashgraph"),
    Asset("ZIL", "Zilliqa"),
    Asset("REP", "Augur"),
    Asset("LSK", "Lisk"),
    Asset("ZB", "ZB"),
    Asset("BOTX", "BOTXCOIN"),
    Asset("CVT", "CyberVein"),
    Asset("ENJ", "Enjin Coin"),
    Asset("EDC", "EDC Blockchain"),
    Asset("DCR", "Decred"),
    Asset("BTG", "Bitcoin Gold"),
    Asset("BAND", "Band Protocol"),
    Asset("DGD", "DigixDAO"),
    Asset("OCEAN", "Ocean Protocol"),
    Asset("SXP", "Swipe"),
    Asset("SC", "Siacoin"),
    Asset("MANA", "Decentraland"),
    Asset("BTM", "Bytom"),
    Asset("RSR", "Reserve Rights Token"),
    Asset("DX", "DxChain Token"),
    Asset("RVN", "Ravencoin"),
    Asset("MONA", "MonaCoin"),
    Asset("BCD", "Bitcoin Diamond"),
    Asset("GNT", "Golem"),
    Asset("QNT", "Quant"),
    Asset("FTM", "Fantom"),
    Asset("IOST", "IOST"),
    Asset("JST", "JUST"),
    Asset("STORJ", "Storj"),
    Asset("SNT", "Status"),
    Asset("BHT", "BHEX Token"),
    Asset("CHSB", "SwissBorg"),
    Asset("KCS", "KuCoin Shares"),
    Asset("RLC", "iExec RLC"),
    Asset("MATIC", "Matic Network"),
    Asset("NEXO", "Nexo"),
    Asset("BTS", "BitShares"),
    Asset("BNT", "Bancor"),
    Asset("HYN", "Hyperion"),
    Asset("CENNZ", "Centrality"),
    Asset("XVG", "Verge"),
    Asset("KMD", "Komodo"),
    Asset("MCO", "Crypto.com"),
    Asset("BTT", "BitTorrent"),
    Asset("STEEM", "Steem"),
    Asset("XWC", "WhiteCoin"),
    Asset("UTK", "UTRUST"),
    Asset("TOMO", "TomoChain"),
    Asset("IRIS", "IRISnet"),
    Asset("ELF", "aelf"),
    Asset("ZEN", "Horizen"),
    Asset("RIF", "RIF Token"),
    Asset("HC", "HyperCash"),
    Asset("CHZ", "Chiliz"),
    Asset("AOA", "Aurora"),
    Asset("ARDR", "Ardor"),
    Asset("ETN", "Electroneum"),
    Asset("WAXP", "WAX"),
    Asset("DIP", "Etherisc"),
    Asset("GNO", "Gnosis"),
    Asset("TRAC", "OriginTrail"),
    Asset("UBT", "Unibright"),
    Asset("IOTX", "IoTeX"),
    Asset("NRG", "Energi"),
    Asset("BXK", "Bitbook Gambling"),
    Asset("XZC", "Zcoin"),
    Asset("MAID", "MaidSafeCoin"),
    Asset("VSYS", "VSYS Coin"),
    Asset("NMR", "Numeraire"),
    Asset("AION", "Aion"),
    Asset("ELA", "Elastos"),
    Asset("TFUEL", "Theta Fuel"),
    Asset("MLN", "Melon"),
    Asset("AE", "Aeternity"),
    Asset("ANKR", "Ankr Network"),
    Asset("POWR", "Power Ledger"),
    Asset("WAN", "Wanchain"),
    Asset("EURS", "STASIS EURS"),
    Asset("ARK", "Ark"),
    Asset("WAX", "WAX"),
    Asset("ENG", "Enigma")
  ).map(a => (a.officialSymbol, a)).toMap

  log.info(s"Total available assets: ${AllAssets.keys}")
}

// TODO later: automatically initialize this list from a reliable network source (coinmarketcap.com etc) and limit it by a minimum market-cap of $100.000.000
