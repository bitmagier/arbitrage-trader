package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{ActorRef, Props}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy
import org.purevalue.arbitrage.{Asset, ExchangeConfig, GlobalConfig, TradePair}
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.Await

case class BitfinexSymbol(currencySymbol: Asset, apiSymbol: String)
case class BitfinexTradePair(baseAsset: Asset, quoteAsset: Asset, apiSymbol: String) extends TradePair

object BitfinexAdapter {
  def props(config: ExchangeConfig): Props = Props(new BitfinexAdapter(config))
}
class BitfinexAdapter(config: ExchangeConfig) extends ExchangeAdapterProxy(config) {
  private val log = LoggerFactory.getLogger(classOf[BitfinexAdapter])

  val baseRestEndpointPublic = "https://api-pub.bitfinex.com"
  val name: String = "BitfinexAdapter"

  var orderBookStreamer: List[ActorRef] = List()

  var bitfinexAssets: Set[BitfinexSymbol] = _
  var bitfinexTradePairs: Set[BitfinexTradePair] = _

  def tradePairs: Set[TradePair] = bitfinexTradePairs.asInstanceOf[Set[TradePair]]

  def initTradePairs(): Unit = {
    import DefaultJsonProtocol._

    val apiSymbolToOfficialCurrencySymbolMapping: Map[String, String] = Await.result(
      queryJson[List[List[Tuple2[String, String]]]](s"$baseRestEndpointPublic/v2/conf/pub:map:currency:sym"),
      config.httpTimeout)
      .head
      .map(e => (e._1, e._2.toUpperCase))
      .toMap
    if (log.isTraceEnabled) log.trace(s"currency mappings received: $apiSymbolToOfficialCurrencySymbolMapping")

    // currency->name
    val currencies: Map[String, String] = Await.result(
      queryJson[List[List[Tuple2[String, String]]]](s"$baseRestEndpointPublic/v2/conf/pub:map:currency:label"),
      config.httpTimeout)
      .head
      .map(e => (e._1, e._2))
      .toMap
    if (log.isTraceEnabled) log.trace(s"currencies received: $currencies")

    bitfinexAssets = currencies // apiSymbol, name
      .map(e => (e._1, apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1))) // apiSymbol, officialSymbol
      .filter(e => GlobalConfig.assets.keySet.contains(e._2)) // global crosscheck
      .filter(e => config.assets.contains(e._2)) // bitfinex config crosscheck
      .map(e => BitfinexSymbol(Asset(e._2), e._1))
      .toSet
    if (log.isTraceEnabled) log.trace(s"bitfinexAssets: $bitfinexAssets")

    val tradePairs: List[String] = Await.result(
      queryJson[List[List[String]]](s"$baseRestEndpointPublic/v2/conf/pub:list:pair:exchange"),
      config.httpTimeout)
      .head
    if (log.isTraceEnabled) log.trace(s"tradepairs: $tradePairs")

    bitfinexTradePairs = tradePairs
      .filter(_.length == 6)
      .map(e => (e.substring(0, 3), e.substring(3, 6), e)) // currency1-apiSymbol, currency2-apiSymbol, tradePair
      .map(e =>
        (apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1), // resolve official currency symbols
          apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._2, e._2), e._3))
      .filter(e =>
        GlobalConfig.assets.keySet.contains(e._1)
          && GlobalConfig.assets.keySet.contains(e._2)) // crosscheck with global assets
      .filter(e =>
        bitfinexAssets.exists(_.currencySymbol.officialSymbol == e._1)
          && bitfinexAssets.exists(_.currencySymbol.officialSymbol == e._2)) // crosscheck with bitfinex (configured) assets
      .map(e => BitfinexTradePair(Asset(e._1), Asset(e._2), e._3))
      .toSet
    if (log.isTraceEnabled) log.trace(s"bitfinexTradePairs: $bitfinexTradePairs")
  }

  override def startStreamingTradePairBasedData(tradePair: TradePair, receipient: ActorRef): Unit = {
    val bitfinexTradePair = bitfinexTradePairs
      .find(e => e.baseAsset == tradePair.baseAsset && e.quoteAsset == tradePair.quoteAsset)
      .getOrElse(throw new RuntimeException(s"No corresponding bitfinex tradepair $tradePair available"))
    orderBookStreamer = orderBookStreamer :+
      context.actorOf(BitfinexOrderBookStreamer.props(config, bitfinexTradePair, receipient), s"BitfinexOrderBookStreamer-$tradePair")
  }

  override def preStart(): Unit = {
    super.preStart()
    initTradePairs()
  }
}
