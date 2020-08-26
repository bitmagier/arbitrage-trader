package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorSystem, Props, Status}
import org.purevalue.arbitrage.Exchange.{GetTradePairs, TradePairs}
import org.purevalue.arbitrage.HttpUtils.httpGetJson
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataChannel._
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}


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

case class BinanceTradePair(baseAsset: Asset, quoteAsset: Asset, symbol: String) extends TradePair

object BinancePublicDataChannel {
  case class GetBinanceTradePair(tradePair: TradePair)

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

  val BaseRestEndpoint = "https://api.binance.com"

  def props(config: ExchangeConfig): Props = Props(new BinancePublicDataChannel(config))
}

/**
 * Binance exchange - account data channel
 */
class BinancePublicDataChannel(config: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinancePublicDataChannel])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private var exchangeInfo: RawBinanceExchangeInformationJson = _
  private var binanceTradePairs: Set[BinanceTradePair] = _

  def tradePairs: Set[TradePair] = binanceTradePairs.asInstanceOf[Set[TradePair]]

  override def preStart(): Unit = {
    import BinanceJsonProtocol._

    exchangeInfo = Await.result(
      httpGetJson[RawBinanceExchangeInformationJson](s"$BaseRestEndpoint/api/v3/exchangeInfo"),
      Config.httpTimeout.plus(500.millis))
    binanceTradePairs = exchangeInfo.symbols
      .filter(s => s.status == "TRADING" && s.orderTypes.contains("LIMIT") /* && s.orderTypes.contains("LIMIT_MAKER")*/ && s.permissions.contains("SPOT"))
      .filter(s => config.assets.contains(s.baseAsset) && config.assets.contains(s.quoteAsset))
      .map(s => BinanceTradePair(Asset(s.baseAsset), Asset(s.quoteAsset), s.symbol))
      .toSet
    log.debug("received ExchangeInfo")
  }

  override def receive: Receive = {
    // Messages from Exchange
    case GetTradePairs() =>
      sender() ! TradePairs(tradePairs)

    // Messages from BinanceTPDataChannel
    case GetBinanceTradePair(tp) =>
      sender() ! binanceTradePairs.find(e => e.baseAsset == tp.baseAsset && e.quoteAsset == tp.quoteAsset).get

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

case class RawBinanceTradePairJson(symbol: String, status: String, baseAsset: String, baseAssetPrecision: Int, quoteAsset: String,
                                   quotePrecision: Int, baseCommissionPrecision: Int, quoteCommissionPrecision: Int,
                                   orderTypes: Seq[String], icebergAllowed: Boolean, ocoAllowed: Boolean,
                                   quoteOrderQtyMarketAllowed: Boolean, isSpotTradingAllowed: Boolean,
                                   isMarginTradingAllowed: Boolean, /*filters*/ permissions: Seq[String])

case class RawBinanceExchangeInformationJson(timezone: String, serverTime: Long, /*rateLimits,exchangeFilters*/ symbols: Seq[RawBinanceTradePairJson])

object BinanceJsonProtocol extends DefaultJsonProtocol {
  implicit val rawSymbolFormat: RootJsonFormat[RawBinanceTradePairJson] = jsonFormat15(RawBinanceTradePairJson)
  implicit val rawExchangeInformationFormat: RootJsonFormat[RawBinanceExchangeInformationJson] = jsonFormat3(RawBinanceExchangeInformationJson)
}
