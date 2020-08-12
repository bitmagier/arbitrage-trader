package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorSystem, Props, Status}
import org.purevalue.arbitrage.Exchange.{GetTradePairs, TradePairs}
import org.purevalue.arbitrage.Utils.queryJson
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexDataChannel.GetBitfinexTradePair
import org.purevalue.arbitrage.{AppConfig, Asset, ExchangeConfig, GlobalConfig, Main, TradePair}
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.{Await, ExecutionContextExecutor}

case class BitfinexSymbol(currencySymbol: Asset, apiSymbol: String)
case class BitfinexTradePair(baseAsset: Asset, quoteAsset: Asset, apiSymbol: String) extends TradePair

object BitfinexDataChannel {
  case class GetBitfinexTradePair(tp:TradePair)

  def props(config: ExchangeConfig): Props = Props(new BitfinexDataChannel(config))
}

/**
 * Bitfinex exchange data channel
 */
class BitfinexDataChannel(config: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexDataChannel])
  implicit val system:ActorSystem = Main.actorSystem
  implicit val executor: ExecutionContextExecutor = system.dispatcher


  val baseRestEndpointPublic = "https://api-pub.bitfinex.com"

  var bitfinexAssets: Set[BitfinexSymbol] = _
  var bitfinexTradePairs: Set[BitfinexTradePair] = _

  def tradePairs: Set[TradePair] = bitfinexTradePairs.asInstanceOf[Set[TradePair]]

  def initTradePairs(): Unit = {
    import DefaultJsonProtocol._

    val apiSymbolToOfficialCurrencySymbolMapping: Map[String, String] =
      Await.result(queryJson[List[List[Tuple2[String, String]]]](s"$baseRestEndpointPublic/v2/conf/pub:map:currency:sym"), AppConfig.httpTimeout)
      .head
      .map(e => (e._1, e._2.toUpperCase))
      .toMap
    if (log.isTraceEnabled) log.trace(s"currency mappings received: $apiSymbolToOfficialCurrencySymbolMapping")

    // currency->name
    val currencies: Map[String, String] =
      Await.result(queryJson[List[List[Tuple2[String, String]]]](s"$baseRestEndpointPublic/v2/conf/pub:map:currency:label"),
      AppConfig.httpTimeout)
      .head
      .map(e => (e._1, e._2))
      .toMap
    if (log.isTraceEnabled) log.trace(s"currencies received: $currencies")

    bitfinexAssets = currencies // apiSymbol, name
      .map(e => (e._1, apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1))) // apiSymbol, officialSymbol
      .filter(e => GlobalConfig.AllAssets.keySet.contains(e._2)) // global crosscheck
      .filter(e => config.assets.contains(e._2)) // bitfinex config crosscheck
      .map(e => BitfinexSymbol(Asset(e._2), e._1))
      .toSet
    if (log.isTraceEnabled) log.trace(s"bitfinexAssets: $bitfinexAssets")

    val tradePairs: List[String] =
      Await.result(queryJson[List[List[String]]](s"$baseRestEndpointPublic/v2/conf/pub:list:pair:exchange"), AppConfig.httpTimeout)
      .head
    if (log.isTraceEnabled) log.trace(s"tradepairs: $tradePairs")

    bitfinexTradePairs = tradePairs
      .filter(_.length == 6)
      .map(e => (e.substring(0, 3), e.substring(3, 6), e)) // currency1-apiSymbol, currency2-apiSymbol, tradePair
      .map(e =>
        (apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1), // resolve official currency symbols
          apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._2, e._2), e._3))
      .filter(e =>
        GlobalConfig.AllAssets.keySet.contains(e._1)
          && GlobalConfig.AllAssets.keySet.contains(e._2)) // crosscheck with global assets
      .filter(e =>
        bitfinexAssets.exists(_.currencySymbol.officialSymbol == e._1)
          && bitfinexAssets.exists(_.currencySymbol.officialSymbol == e._2)) // crosscheck with bitfinex (configured) assets
      .map(e => BitfinexTradePair(Asset(e._1), Asset(e._2), e._3))
      .toSet
    if (log.isTraceEnabled) log.trace(s"bitfinexTradePairs: $bitfinexTradePairs")
  }

  override def preStart(): Unit = {
    initTradePairs()
  }

  override def receive: Receive = {
    // Messages from Exchange
    case GetTradePairs() =>
      sender() ! TradePairs(tradePairs)

    // Messages from BitfinexTPDataChannel
    case GetBitfinexTradePair(tp) =>
      sender() ! bitfinexTradePairs.find(e => e.baseAsset==tp.baseAsset && e.quoteAsset==tp.quoteAsset).get

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
