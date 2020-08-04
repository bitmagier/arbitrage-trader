package org.purevalue.arbitrage.adapter.binance

import akka.actor.{ActorRef, Props, Status}
import akka.pattern.pipe
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy
import org.purevalue.arbitrage.adapter.binance.BinanceAdapter.{GetOrderBookSnapshot, baseEndpoint}
import org.purevalue.arbitrage.{Asset, ExchangeConfig, TradePair}
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.Await


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

object BinanceAdapter {
  case class GetOrderBookSnapshot(tradePair: BinanceTradePair)

  val baseEndpoint = "https://api.binance.com"

  def props(config: ExchangeConfig): Props = Props(new BinanceAdapter(config))
}

class BinanceAdapter(config: ExchangeConfig) extends ExchangeAdapterProxy(config) {
  private val log = LoggerFactory.getLogger(classOf[BinanceAdapter])

  val name: String = "BinanceAdapter"

  private var exchangeInfo: RawBinanceExchangeInformation = _
  private var orderBookStreamer: List[ActorRef] = List()
  private var binanceTradePairs: Set[BinanceTradePair] = _

  override def tradePairs: Set[TradePair] = binanceTradePairs.asInstanceOf[Set[TradePair]]


  override def preStart(): Unit = {
    import BinanceJsonProtocol._
    super.preStart()
    exchangeInfo = Await.result(queryJson[RawBinanceExchangeInformation](s"$baseEndpoint/api/v3/exchangeInfo"), config.httpTimeout)
    binanceTradePairs = exchangeInfo.symbols
      .filter(s => s.status=="TRADING" && s.orderTypes.contains("LIMIT") /* && s.orderTypes.contains("LIMIT_MAKER")*/ && s.permissions.contains("SPOT"))
      .filter(s => config.assets.contains(s.baseAsset) && config.assets.contains(s.quoteAsset))
      .map(s => BinanceTradePair(Asset(s.baseAsset), Asset(s.quoteAsset), s.symbol))
      .toSet
    log.debug("received ExchangeInfo")
  }

  override def startStreamingOrderBook(tradePair: TradePair, receipient: ActorRef): Unit = {
    val binanceTradePair = binanceTradePairs
      .find(e => e.baseAsset == tradePair.baseAsset && e.quoteAsset == tradePair.quoteAsset)
      .getOrElse(throw new RuntimeException(s"No binance tradepair $tradePair available"))
    orderBookStreamer = orderBookStreamer :+
      context.actorOf(BinanceOrderBookStreamer.props(config, binanceTradePair, self, receipient), s"BinanceOrderBookStreamer-$tradePair")
  }

  override def receive: Receive = super.receive orElse {

    // Messages from BinanceOrderBookStreamer

    case GetOrderBookSnapshot(tradePair) =>
      import BinanceJsonProtocol._
      log.debug(s"Binance: Get OrderBookSnapshot for $tradePair")

      queryJson[RawOrderBookSnapshot](s"$baseEndpoint/api/v3/depth?symbol=${tradePair.symbol}&limit=1000")
        .pipeTo(sender())

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

case class RawBinanceTradePair(symbol: String, status: String, baseAsset: String, baseAssetPrecision: Int, quoteAsset: String,
                               quotePrecision: Int, baseCommissionPrecision: Int, quoteCommissionPrecision: Int,
                               orderTypes: Seq[String], icebergAllowed: Boolean, ocoAllowed: Boolean,
                               quoteOrderQtyMarketAllowed: Boolean, isSpotTradingAllowed: Boolean,
                               isMarginTradingAllowed: Boolean, /*filters*/ permissions: Seq[String])

case class RawBinanceExchangeInformation(timezone: String, serverTime: Long, /*rateLimits,exchangeFilters*/ symbols: Seq[RawBinanceTradePair])
case class RawOrderBookSnapshot(lastUpdateId: Long, bids: Seq[Seq[String]], asks: Seq[Seq[String]])

object BinanceJsonProtocol extends DefaultJsonProtocol {
  implicit val rawSymbolFormat: RootJsonFormat[RawBinanceTradePair] = jsonFormat15(RawBinanceTradePair)
  implicit val rawExchangeInformationFormat: RootJsonFormat[RawBinanceExchangeInformation] = jsonFormat3(RawBinanceExchangeInformation)
  implicit val orderBookSnapshot: RootJsonFormat[RawOrderBookSnapshot] = jsonFormat3(RawOrderBookSnapshot)
}
