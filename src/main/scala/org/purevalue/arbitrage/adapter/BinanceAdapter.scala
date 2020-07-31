package org.purevalue.arbitrage.adapter

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest}
import org.purevalue.arbitrage.adapter.BinanceAdapter.baseEndpoint
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.{GetTradePairs, OrderBookStreamRequest, TradePairs}
import org.purevalue.arbitrage.{ExchangeConfig, Main, TradePair}
import spray.json._

import scala.concurrent.{Await, ExecutionContextExecutor, Future}


object ExchangeQueryAdapter {
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class OrderBookStreamRequest(tradePair: TradePair)
}

abstract class ExchangeQueryAdapter extends Actor {
  def name: String
  def tradePairs: Set[TradePair]
  def startStreamingOrderBook(tradePair: TradePair, receipient: ActorRef): Unit

  override def receive: Receive = {
    case GetTradePairs => sender() ! TradePairs(tradePairs)
    case OrderBookStreamRequest(tradePair) => startStreamingOrderBook(tradePair, sender())
  }
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
object BinanceAdapter {
  val baseEndpoint = "https://api.binance.com"

  def props(config:ExchangeConfig): Props = Props(new BinanceAdapter(config))
}

class BinanceAdapter(config: ExchangeConfig) extends ExchangeQueryAdapter {
  private val log = Logging(context.system, this)
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val name: String = "BinanceAdapter"

  private var exchangeInfo: BinanceExchangeInformation = _
  private var orderBookStreamer: List[ActorRef] = List()
  var tradePairs: Set[TradePair] = _

  private def queryExchangeInfo(): Future[BinanceExchangeInformation] = {
    import BinanceJsonProtocol._

    log.info(s"refreshing $name ExchangeInfo ...")
    val responseFuture = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = s"$baseEndpoint/api/v3/exchangeInfo"
      ))
    responseFuture.flatMap(_.entity.toStrict(config.httpTimeout)).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` => JsonParser(r.data.utf8String).convertTo[BinanceExchangeInformation]
        case _ =>
          throw new Exception(s"Failed to parse ExchangeInfo query response:\n${r.data.utf8String}")
      }
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    exchangeInfo = Await.result(queryExchangeInfo(), config.httpTimeout)
    tradePairs = exchangeInfo.symbols
      .map(s => TradePair(s.symbol, s.baseAsset, s.quoteAsset))
      .filter(e => config.assets.contains(e.baseAsset) && config.assets.contains(e.quoteAsset))
      .toSet
    log.debug("received ExchangeInfo")
  }

  override def startStreamingOrderBook(tradePair: TradePair, receipient: ActorRef): Unit = {
    orderBookStreamer = orderBookStreamer :+
      context.actorOf(BinanceOrderBookStreamer.props(config, tradePair, receipient), s"BinanceOrderBookStreamer-${tradePair.symbol}")
  }
}

case class BinanceSymbol(symbol: String, status: String, baseAsset: String, baseAssetPrecision: Int, quoteAsset: String,
                         quotePrecision: Int, baseCommissionPrecision: Int, quoteCommissionPrecision: Int,
                         orderTypes: Seq[String], icebergAllowed: Boolean, ocoAllowed: Boolean,
                         quoteOrderQtyMarketAllowed: Boolean, isSpotTradingAllowed: Boolean,
                         isMarginTradingAllowed: Boolean, /*filters*/ permissions: Seq[String])

case class BinanceExchangeInformation(timezone: String, serverTime: Long, /*rateLimits,exchangeFilters*/ symbols: Seq[BinanceSymbol])

object BinanceJsonProtocol extends DefaultJsonProtocol {
  implicit val symbolFormat: RootJsonFormat[BinanceSymbol] = jsonFormat15(BinanceSymbol)
  implicit val exchangeInformationFormat: RootJsonFormat[BinanceExchangeInformation] = jsonFormat3(BinanceExchangeInformation)
}
