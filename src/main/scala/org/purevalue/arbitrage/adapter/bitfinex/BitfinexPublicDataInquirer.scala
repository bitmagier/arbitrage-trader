package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.{GetBitfinexAssets, GetBitfinexTradePairs}
import org.purevalue.arbitrage.traderoom.exchange.Exchange.GetAllTradePairs
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

private[bitfinex] case class BitfinexAsset(asset: Asset, apiSymbol: String)
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
                                                   exchangeConfig: ExchangeConfig) extends Actor with ActorLogging {
  private implicit val system: ActorSystem = Main.actorSystem
  private implicit val executor: ExecutionContextExecutor = system.dispatcher

  val BaseRestEndpointPublic = "https://api-pub.bitfinex.com"

  var bitfinexAssets: Set[BitfinexAsset] = _
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

    if (log.isDebugEnabled) log.debug(s"currency mappings received: $apiSymbolToOfficialCurrencySymbolMapping")

    // currency->verbose name
    val currencyNames: Map[String, String] =
      Await.result(
        httpGetJson[List[List[Tuple2[String, String]]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:map:currency:label"),
        globalConfig.httpTimeout.plus(500.millis)) match {
        case Left(response) =>
          response.head
            .map(e => (e._1, e._2))
            .toMap
        case Right(errorResponse) => throw new RuntimeException(s"query currency labels failed: $errorResponse")
      }

    if (log.isDebugEnabled) log.debug(s"pub:map:currency:label: $currencyNames")

    val rawBitfinexCurrencies: Set[String] = Await.result(
      httpGetJson[List[List[String]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:list:currency"),
      globalConfig.httpTimeout.plus(500.millis)) match {
      case Left(response) => response.head.toSet
      case Right(errorResponse) => throw new RuntimeException(s"query currencies failed: $errorResponse")
    }

    rawBitfinexCurrencies.foreach { e =>
      val officialSymbol = apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e, e)
      val name: Option[String] = currencyNames.get(e)
      Asset.register(officialSymbol, name, None)
    }

    bitfinexAssets = rawBitfinexCurrencies.map { e =>
      val officialSymbol = apiSymbolToOfficialCurrencySymbolMapping.getOrElse(e, e)
      BitfinexAsset(Asset(officialSymbol), e)
    }

    if (log.isDebugEnabled) log.debug(s"bitfinex assets: $bitfinexAssets")

    val rawTradePairs: List[String] =
      Await.result(
        httpGetJson[List[List[String]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:list:pair:exchange"),
        globalConfig.httpTimeout) match {
        case Left(response) => response.head
        case Right(errorResponse) => throw new RuntimeException(s"query exchange pairs failed: $errorResponse")
      }
    if (log.isDebugEnabled) log.debug(s"pub:list:pair:exchange: $rawTradePairs")

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
    if (log.isDebugEnabled) log.debug(s"bitfinex trade pairs: $bitfinexTradePairs")
  }

  def init(): Unit = {
    try {
      initTradePairs()
    } catch {
      case e: Exception => log.error(e, "init failed")
    }
  }

  override def preStart(): Unit = {
    init()
  }

  override def receive: Receive = {
    // Messages from Exchange
    case GetAllTradePairs() =>
      sender() ! tradePairs

    case GetBitfinexTradePairs() =>
      sender() ! bitfinexTradePairs

    case GetBitfinexAssets() =>
      sender() ! bitfinexAssets
  }
}
