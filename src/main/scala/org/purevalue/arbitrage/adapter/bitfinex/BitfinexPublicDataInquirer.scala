package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.PublicDataInquirer
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.{GetBitfinexAssets, GetBitfinexTradePairs}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

private[bitfinex] case class BitfinexAsset(asset: Asset, apiSymbol: String)
private[bitfinex] case class BitfinexTradePair(baseAsset: Asset, quoteAsset: Asset, apiSymbol: String) {
  def toTradePair: TradePair = TradePair(baseAsset, quoteAsset)
}


object BitfinexPublicDataInquirer {
  def apply(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig):
  Behavior[PublicDataInquirer.Command] =
    Behaviors.setup(context => new BitfinexPublicDataInquirer(context, globalConfig, exchangeConfig))

  case class GetBitfinexTradePairs(replyTo: ActorRef[Set[BitfinexTradePair]]) extends PublicDataInquirer.Command
  case class GetBitfinexAssets(replyTo: ActorRef[Set[BitfinexAsset]]) extends PublicDataInquirer.Command
}

/**
 * Bitfinex exchange data channel
 */
private[bitfinex] class BitfinexPublicDataInquirer(context: ActorContext[PublicDataInquirer.Command],
                                                   globalConfig: GlobalConfig,
                                                   exchangeConfig: ExchangeConfig) extends PublicDataInquirer(context) {

  import PublicDataInquirer._

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

    if (context.log.isDebugEnabled) context.log.debug(s"currency mappings received: $apiSymbolToOfficialCurrencySymbolMapping")

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

    if (context.log.isDebugEnabled) context.log.debug(s"pub:map:currency:label: $currencyNames")

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

    if (context.log.isDebugEnabled) context.log.debug(s"bitfinex assets: $bitfinexAssets")

    val rawTradePairs: List[String] =
      Await.result(
        httpGetJson[List[List[String]], JsValue](s"$BaseRestEndpointPublic/v2/conf/pub:list:pair:exchange"),
        globalConfig.httpTimeout) match {
        case Left(response) => response.head
        case Right(errorResponse) => throw new RuntimeException(s"query exchange pairs failed: $errorResponse")
      }
    if (context.log.isDebugEnabled) context.log.debug(s"pub:list:pair:exchange: $rawTradePairs")

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
    if (context.log.isDebugEnabled) context.log.debug(s"bitfinex trade pairs: $bitfinexTradePairs")
  }

  override def onMessage(message: Command): Behavior[Command] = {
    message match {
      // @formatter:off
      case GetAllTradePairs(replyTo)      => replyTo ! tradePairs
      case GetBitfinexTradePairs(replyTo) => replyTo ! bitfinexTradePairs
      case GetBitfinexAssets(replyTo)     => replyTo ! bitfinexAssets
      // @formatter:on
    }
    this
  }

  initTradePairs()
}
