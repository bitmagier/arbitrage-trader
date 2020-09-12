package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorSystem, Props, Status}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.{GetBitfinexAssets, GetBitfinexTradePairs}
import org.purevalue.arbitrage.traderoom.Exchange.{GetTradePairs, TradePairs}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

case class BitfinexSymbol(asset: Asset, apiSymbol: String)
case class BitfinexTradePair(baseAsset: Asset, quoteAsset: Asset, apiSymbol: String) extends TradePair

object BitfinexPublicDataInquirer {
  case class GetBitfinexTradePairs()
  case class GetBitfinexAssets()

  def props(config: ExchangeConfig): Props = Props(new BitfinexPublicDataInquirer(config))
}

/**
 * Bitfinex exchange data channel
 */
class BitfinexPublicDataInquirer(config: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexPublicDataInquirer])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  val BaseRestEndpointPublic = "https://api-pub.bitfinex.com"

  var bitfinexAssets: Set[BitfinexSymbol] = _
  var bitfinexTradePairs: Set[BitfinexTradePair] = _

  def tradePairs: Set[TradePair] = bitfinexTradePairs.asInstanceOf[Set[TradePair]]

  def initTradePairs(): Unit = {
    import DefaultJsonProtocol._

    val apiSymbolToOfficialCurrencySymbolMapping: Map[String, String] =
      Await.result(
        httpGetJson[List[List[Tuple2[String, String]]]](s"$BaseRestEndpointPublic/v2/conf/pub:map:currency:sym"),
        Config.httpTimeout.plus(500.millis))
        .head
        .map(e => (e._1, e._2.toUpperCase))
        .toMap
    if (log.isTraceEnabled) log.trace(s"currency mappings received: $apiSymbolToOfficialCurrencySymbolMapping")

    // currency->name
    val currencies: Map[String, String] =
      Await.result(
        httpGetJson[List[List[Tuple2[String, String]]]](s"$BaseRestEndpointPublic/v2/conf/pub:map:currency:label"),
        Config.httpTimeout.plus(500.millis))
        .head
        .map(e => (e._1, e._2))
        .toMap
    if (log.isTraceEnabled) log.trace(s"currencies received: $currencies")

    bitfinexAssets = currencies // apiSymbol, name
      .map(e => (e._1, apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1))) // apiSymbol, officialSymbol
      .filter(e => StaticConfig.AllAssets.keySet.contains(e._2)) // global crosscheck
      .map(e => BitfinexSymbol(Asset(e._2), e._1))
      .toSet
    if (log.isTraceEnabled) log.trace(s"bitfinexAssets: $bitfinexAssets")

    val tradePairs: List[String] =
      Await.result(httpGetJson[List[List[String]]](s"$BaseRestEndpointPublic/v2/conf/pub:list:pair:exchange"), Config.httpTimeout)
        .head
    if (log.isTraceEnabled) log.trace(s"tradepairs: $tradePairs")

    bitfinexTradePairs = tradePairs
      .filter(_.length == 6)
      .map(e => (e.substring(0, 3), e.substring(3, 6), e)) // currency1-apiSymbol, currency2-apiSymbol, tradePair
      .map(e =>
        (apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1), // resolve official currency symbols
          apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._2, e._2), e._3))
      .filter(e =>
        StaticConfig.AllAssets.keySet.contains(e._1)
          && StaticConfig.AllAssets.keySet.contains(e._2)) // crosscheck with global assets
      .filter(e =>
        bitfinexAssets.exists(_.asset.officialSymbol == e._1)
          && bitfinexAssets.exists(_.asset.officialSymbol == e._2)) // crosscheck with bitfinex (configured) assets
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
    case GetBitfinexTradePairs() =>
      sender() ! bitfinexTradePairs

    case GetBitfinexAssets() =>
      sender() ! bitfinexAssets

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
