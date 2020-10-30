package org.purevalue.arbitrage.adapter.binance

import akka.actor.typed.ActorRef
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer._
import org.purevalue.arbitrage.traderoom.exchange.Exchange.GetAllTradePairs
import org.purevalue.arbitrage.traderoom.exchange.{Ask, Bid}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.purevalue.arbitrage.util.Util.stepSizeToFractionDigits
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}


// The LOT_SIZE filter defines the quantity rules for the symbol
private[binance] case class LotSize(minQty: Double,
                                    maxQty: Double,
                                    stepSize: Double) // stepSize defines the intervals that a quantity/icebergQty can be increased/decreased by
private[binance] case class LotSizeJson(minQty: String,
                                        maxQty: String,
                                        stepSize: String) {
  def toLotSize: LotSize = LotSize(minQty.toDouble, maxQty.toDouble, stepSize.toDouble)
}

private[binance] case class RawBinanceTradePairJson(symbol: String, status: String, baseAsset: String, baseAssetPrecision: Int, quoteAsset: String,
                                                    quotePrecision: Int, baseCommissionPrecision: Int, quoteCommissionPrecision: Int,
                                                    orderTypes: Seq[String], icebergAllowed: Boolean, ocoAllowed: Boolean,
                                                    quoteOrderQtyMarketAllowed: Boolean, isSpotTradingAllowed: Boolean,
                                                    isMarginTradingAllowed: Boolean, filters: Seq[JsObject], permissions: Seq[String])

private[binance] case class RawBinanceExchangeInformationJson(timezone: String, serverTime: Long, rateLimits: Seq[JsObject], exchangeFilters: Seq[JsObject], symbols: Seq[RawBinanceTradePairJson])

private[binance] object BinanceJsonProtocol extends DefaultJsonProtocol {
  implicit val rawSymbolFormat: RootJsonFormat[RawBinanceTradePairJson] = jsonFormat16(RawBinanceTradePairJson)
  implicit val rawExchangeInformationFormat: RootJsonFormat[RawBinanceExchangeInformationJson] = jsonFormat5(RawBinanceExchangeInformationJson)
  implicit val lotSizeJsonFormat: RootJsonFormat[LotSizeJson] = jsonFormat3(LotSizeJson)
}


/*
 * Binance General API Information
 * The base endpoint is: https://api.binance.com
 * All endpoints return either a JSON object or array.
 * Data is returned in ascending order. Oldest first, newest last.
 * All time and timestamp related fields are in milliseconds.
 *
 * HTTP Return Codes
 * HTTP 4XX return codes are used for malformed requests; the issue is on the sender's side.
 * HTTP 403 return code is used when the WAF Limit (Web Application Firewall) has been violated.
 * HTTP 429 return code is used when breaking a request rate limit.
 * HTTP 418 return code is used when an IP has been auto-banned for continuing to send requests after receiving 429 codes.
 * HTTP 5XX return codes are used for internal errors; the issue is on Binance's side. It is important to NOT treat this as a failure operation; the execution status is UNKNOWN and could have been a success.
 */

private[binance] case class BinanceTradePair(baseAsset: Asset,
                                             quoteAsset: Asset,
                                             symbol: String,
                                             baseAssetPrecision: Int,
                                             quotePrecision: Int,
                                             tickSize: Double, // price_filter: tickSize defines the intervals that a price/stopPrice can be increased/decreased by; disabled on tickSize == 0.
                                             lotSize: LotSize,
                                             minNotional: Double) {
  def toTradePair: TradePair = TradePair(baseAsset, quoteAsset)
}

object BinancePublicDataInquirer {

  sealed trait Command
  case class GetBinanceTradePairs(replyTo: ActorRef[_]) extends Command

  def toBid(e: Seq[String]): Bid = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    Bid(
      e.head.toDouble, // Price level
      e(1).toDouble // Quantity
    )
  }

  def toAsk(e: Seq[String]): Ask = {
    if (e.length != 2) throw new IllegalArgumentException(e.toString())
    Ask(
      e.head.toDouble, // Price level
      e(1).toDouble // Quantity
    )
  }

  val BinanceBaseRestEndpoint = "https://api.binance.com"

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig): Props =
    Props(new BinancePublicDataInquirer(globalConfig, exchangeConfig))
}

/**
 * Binance exchange - account data channel
 */
private[binance] class BinancePublicDataInquirer(globalConfig: GlobalConfig,
                                                 exchangeConfig: ExchangeConfig) extends Actor with ActorLogging {
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var exchangeInfo: RawBinanceExchangeInformationJson = _
  var binanceTradePairs: Set[BinanceTradePair] = _

  def tradePairs: Set[TradePair] = binanceTradePairs.map(_.toTradePair)

  def init(): Unit = {
    import BinanceJsonProtocol._

    try {
      exchangeInfo = Await.result(
        httpGetJson[RawBinanceExchangeInformationJson, JsValue](s"$BinanceBaseRestEndpoint/api/v3/exchangeInfo"),
        globalConfig.httpTimeout.plus(500.millis)) match {
        case Left(response) => response
        case Right(errorResponse) => throw new RuntimeException(s"query exchange info failed: $errorResponse")
      }

      val rawBinanceTradePairs = exchangeInfo.symbols
        .filter(s => s.status == "TRADING" && s.orderTypes.contains("LIMIT") && s.permissions.contains("SPOT"))
        .map(s => (
          s.baseAsset,
          s.quoteAsset,
          s.symbol,
          s.baseAssetPrecision,
          s.quotePrecision,
          s.filters.find(_.fields("filterType") == JsString("PRICE_FILTER")).get.fields("tickSize").convertTo[String].toDouble,
          s.filters.find(_.fields("filterType") == JsString("LOT_SIZE")).get.convertTo[LotSizeJson].toLotSize,
          s.filters.find(_.fields("filterType") == JsString("MIN_NOTIONAL")).get.fields("minNotional").convertTo[String].toDouble
        ))

      val baseAssetsToRegister: Map[String, Int] =
        rawBinanceTradePairs.groupBy(_._1)
          .map(e => (e._1, stepSizeToFractionDigits(e._2.head._7.stepSize)))
      val furtherQuoteAssetsToRegister: Set[String] =
        rawBinanceTradePairs.map(_._2).toSet -- baseAssetsToRegister.keys

      baseAssetsToRegister.foreach { e =>
        Asset.register(e._1, None, None, e._2, exchangeConfig.assetSourceWeight)
      }
      furtherQuoteAssetsToRegister.foreach { e =>
        Asset.register(e, None, None)
      }

      binanceTradePairs = rawBinanceTradePairs
        .map(e => BinanceTradePair(Asset(e._1), Asset(e._2), e._3, e._4, e._5, e._6, e._7, e._8))
        .filterNot(e => exchangeConfig.assetBlocklist.contains(e.baseAsset) || exchangeConfig.assetBlocklist.contains(e.quoteAsset))
        .toSet

      if (log.isDebugEnabled) log.debug("received ExchangeInfo")
    } catch {
      case e: Exception => log.error(e, "init failed")
    }
  }


  override def preStart(): Unit = {
    init()
  }

  override def receive: Receive = {
    // @formatter:off
    case GetAllTradePairs()     => sender() ! tradePairs // from exchange
    case GetBinanceTradePairs() => sender() ! binanceTradePairs // from BinancePublicDataChannel
    // @formatter:on
  }
}


/* sample RawBinanceTradePairJson for symbol=BTCUSDT:
{
  "baseAsset": "BTC",
  "baseAssetPrecision": 8,
  "baseCommissionPrecision": 8,
  "filters": [{
  "filterType": "PRICE_FILTER",
  "maxPrice": "1000000.00000000",
  "minPrice": "0.01000000",
  "tickSize": "0.01000000"
}, {
  "avgPriceMins": 5,
  "filterType": "PERCENT_PRICE",
  "multiplierDown": "0.2",
  "multiplierUp": "5"
}, {
  "filterType": "LOT_SIZE",
  "maxQty": "9000.00000000",
  "minQty": "0.00000100",
  "stepSize": "0.00000100"
}, {
  "applyToMarket": true,
  "avgPriceMins": 5,
  "filterType": "MIN_NOTIONAL",
  "minNotional": "10.00000000"
}, {
  "filterType": "ICEBERG_PARTS",
  "limit": 10
}, {
  "filterType": "MARKET_LOT_SIZE",
  "maxQty": "394.27164616",
  "minQty": "0.00000000",
  "stepSize": "0.00000000"
}, {
  "filterType": "MAX_NUM_ALGO_ORDERS",
  "maxNumAlgoOrders": 5
}, {
  "filterType": "MAX_NUM_ORDERS",
  "maxNumOrders": 200
}],
  "icebergAllowed": true,
  "isMarginTradingAllowed": true,
  "isSpotTradingAllowed": true,
  "ocoAllowed": true,
  "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"],
  "permissions": ["SPOT", "MARGIN"],
  "quoteAsset": "USDT",
  "quoteCommissionPrecision": 8,
  "quoteOrderQtyMarketAllowed": true,
  "quotePrecision": 8,
  "status": "TRADING",
  "symbol": "BTCUSDT"
}
*/