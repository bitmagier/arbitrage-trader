package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorSystem, Props, Status}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.{GetBitfinexAssets, GetBitfinexTradePairs}
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{GetTickerTradePairs, TradePairs}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

private[bitfinex] case class BitfinexSymbol(asset: Asset, apiSymbol: String)
private[bitfinex] case class BitfinexTradePair(baseAsset: Asset, quoteAsset: Asset, apiSymbol: String) {
  def toTradePair: TradePair = TradePair(baseAsset, quoteAsset)
}


object BitfinexPublicDataInquirer {
  case class GetBitfinexTradePairs()
  case class GetBitfinexAssets()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig): Props =
    Props(new BitfinexPublicDataInquirer(globalConfig, exchangeConfig))
}

/**
 * Bitfinex exchange data channel
 */
private[bitfinex] class BitfinexPublicDataInquirer(globalConfig: GlobalConfig,
                                                   exchangeConfig: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexPublicDataInquirer])
  private implicit val system: ActorSystem = Main.actorSystem
  private implicit val executor: ExecutionContextExecutor = system.dispatcher

  val BaseRestEndpointPublic = "https://api-pub.bitfinex.com"

  var bitfinexAssets: Set[BitfinexSymbol] = _
  var bitfinexTradePairs: Set[BitfinexTradePair] = _

  def tradePairs: Set[TradePair] = bitfinexTradePairs.map(_.toTradePair)

  def initTradePairs(): Unit = {
    import DefaultJsonProtocol._

    val apiSymbolToOfficialCurrencySymbolMapping: Map[String, String] =
      Await.result(
        httpGetJson[List[List[Tuple2[String, String]]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:map:currency:sym"),
        globalConfig.httpTimeout.plus(500.millis)) match {
        case Left(response) =>
          response.head
            .map(e => (e._1, e._2.toUpperCase))
            .toMap
        case Right(errorResponse) => throw new RuntimeException(s"query currency symbols failed: $errorResponse")
      }

    if (log.isTraceEnabled) log.trace(s"currency mappings received: $apiSymbolToOfficialCurrencySymbolMapping")

    // currency->name
    val currencies: Map[String, String] =
      Await.result(
        httpGetJson[List[List[Tuple2[String, String]]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:map:currency:label"),
        globalConfig.httpTimeout.plus(500.millis)) match {
        case Left(response) =>
          response.head
            .map(e => (e._1, e._2))
            .toMap
        case Right(errorResponse) => throw new RuntimeException(s"query currency labels failed: $errorResponse")
      }

    if (log.isTraceEnabled) log.trace(s"currencies received: $currencies")

    val rawBitfinexAssets = currencies // apiSymbol, name
      .map(e => (e._1, apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1))) // apiSymbol, officialSymbol

    rawBitfinexAssets.foreach { e =>
      Asset.register(e._2, None, None)
    }

    bitfinexAssets = rawBitfinexAssets
      .map(e => BitfinexSymbol(Asset(e._2), e._1))
      .toSet

    if (log.isTraceEnabled) log.trace(s"bitfinexAssets: $bitfinexAssets")

    val rawTradePairs: List[String] =
      Await.result(
        httpGetJson[List[List[String]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:list:pair:exchange"),
        globalConfig.httpTimeout) match {
        case Left(response) => response.head
        case Right(errorResponse) => throw new RuntimeException(s"query exchange pairs failed: $errorResponse")
      }
    if (log.isTraceEnabled) log.trace(s"tradepairs: $rawTradePairs")

    bitfinexTradePairs = rawTradePairs
      .filter(_.length == 6)
      .map(e => (e.substring(0, 3), e.substring(3, 6), e)) // currency1-apiSymbol, currency2-apiSymbol, tradePair
      .map(e =>
        (apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._1, e._1), // resolve official currency symbols
          apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e._2, e._2), e._3))
      .filter(e =>
        bitfinexAssets.exists(_.asset.officialSymbol == e._1)
          && bitfinexAssets.exists(_.asset.officialSymbol == e._2)) // crosscheck with bitfinex (configured) assets
      .map(e => BitfinexTradePair(Asset(e._1), Asset(e._2), s"t${e._3}"))
      .filterNot(e => exchangeConfig.assetBlocklist.contains(e.baseAsset) || exchangeConfig.assetBlocklist.contains(e.quoteAsset))
      .toSet
    if (log.isTraceEnabled) log.trace(s"bitfinexTradePairs: $bitfinexTradePairs")
  }

  override def preStart(): Unit = {
    try {
      initTradePairs()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  override def receive: Receive = {
    // Messages from Exchange
    case GetTickerTradePairs() =>
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
