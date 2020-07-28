package org.purevalue.arbitrage.adapter

import akka.actor.{Actor, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import org.purevalue.arbitrage.adapter.ExchangeQueryAdapter.GetTradePairs
import org.purevalue.arbitrage.{Main, TradePair}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object ExchangeQueryAdapter {
  case class GetTradePairs()
  case class OrderBookInitRequest()
}

abstract class ExchangeQueryAdapter extends Actor {
  def name: String
  def queryTradePairs(): Seq[TradePair]

  override def receive: Receive = {
    case GetTradePairs =>
      sender() ! queryTradePairs()
  }
}


case class BinanceSymbol(symbol: String, status: String, baseAsset: String, baseAssetPrecision: Int, quoteAsset: String,
                         quotePrecision: Int, baseCommissionPrecision: Int, quoteCommissionPrecision: Int,
                         orderTypes: Seq[String], icebergAllowed: Boolean, ocoAllowed: Boolean,
                         quoteOrderQtyMarketAllowed: Boolean, isSpotTradingAllowed: Boolean,
                         isMarginTradingAllowed: Boolean, /*filters*/ permissions: Seq[String])
case class BinanceExchangeInformation(timezone:String, serverTime:Long, /*rateLimits,exchangeFilters*/ symbols:Seq[BinanceSymbol])

object BinanceJsonProtocol extends DefaultJsonProtocol {
  implicit val symbolFormat: RootJsonFormat[BinanceSymbol] = jsonFormat15(BinanceSymbol)
  implicit val exchangeInformationFormat: RootJsonFormat[BinanceExchangeInformation] = jsonFormat3(BinanceExchangeInformation)
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
class BinanceAdapter extends ExchangeQueryAdapter {
  private val log = Logging(context.system, this)
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val name: String = "BinanceAdapter"
  private val baseEndpoint = "https://api.binance.com"

  private var _exchangeInfo:Future[BinanceExchangeInformation] = _

  def exchangeInfo:Future[BinanceExchangeInformation] = {
    import BinanceJsonProtocol._
    if (_exchangeInfo == null) {
      log.info(s"refreshing $name ExchangeInfo ...")
      val responseFuture = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.GET,
          uri = s"$baseEndpoint/api/v3/exchangeInfo"
          //protocol = HttpProtocols.`HTTP/2.0`
        ))
      _exchangeInfo = responseFuture.flatMap(_.entity.toStrict(2.seconds)).map { e =>
        JsonParser(e.data.utf8String).convertTo[BinanceExchangeInformation]
      }
      log.info(s"refreshed $name ExchangeInfo")
    }
    _exchangeInfo
  }

  def queryTradePairs(): Seq[TradePair] = {
    log.debug("queryTradePairs")
    val info = Await.ready(exchangeInfo, 10.seconds)
    var result: Seq[TradePair] = null
    info.onComplete {
      case Success(value) => result = value.symbols.map(s => TradePair(s.symbol, s.baseAsset, s.quoteAsset))
      case Failure(exception) =>
        log.error(exception, "queryTradePairs failed")
        throw exception
        // TODO what to do with the expected actor reply?
    }
    result
  }
}
