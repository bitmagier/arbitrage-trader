package org.purevalue.arbitrage.adapter.coinbase

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataInquirer.GetCoinbaseTradePairs
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{GetTradePairs, TradePairs}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.{Await, ExecutionContextExecutor}

private[coinbase] case class CoinbaseTradePair(id: String, // product_id
                                               baseAsset: Asset,
                                               quoteAsset: Asset,
                                               baseIncrement: Double,
                                               quoteIncrement: Double,
                                               baseMinSize: Double) {
  def toTradePair: TradePair = TradePair(baseAsset, quoteAsset)
}

private[coinbase] case class ProductJson(id: String,
                                         base_currency: String,
                                         quote_currency: String,
                                         base_increment: String,
                                         quote_increment: String,
                                         base_min_size: String,
                                         base_max_size: String,
                                         status: String, // "online"
                                         status_message: String,
                                         cancel_only: Boolean,
                                         limit_only: Boolean,
                                         post_only: Boolean,
                                         trading_disabled: Boolean) {
  def toCoinbaseTradePair: CoinbaseTradePair = CoinbaseTradePair(
    id,
    Asset(base_currency),
    Asset(quote_currency),
    base_increment.toDouble,
    quote_increment.toDouble,
    base_min_size.toDouble
  )
}

private[coinbase] object CoinbaseJsonProtocol extends DefaultJsonProtocol {
  implicit val productJson: RootJsonFormat[ProductJson] = jsonFormat13(ProductJson)
}

object CoinbasePublicDataInquirer {
  case class GetCoinbaseTradePairs()
  case class DeliverAccounts()

  case class CoinbaseTradePair()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig): Props = Props(new CoinbasePublicDataInquirer(globalConfig, exchangeConfig))
}
private[coinbase] class CoinbasePublicDataInquirer(globalConfig: GlobalConfig,
                                                   exchangeConfig: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[CoinbasePublicDataInquirer])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val CoinbaseBaseRestEndpoint: String = "https://api.pro.coinbase.com"

  var tradePairs: Set[TradePair] = _
  var coinbaseTradePairs: Set[CoinbaseTradePair] = _

  import CoinbaseJsonProtocol._

  def pullTradePairs(): Unit = {
    coinbaseTradePairs =
      Await.result(
        HttpUtil.httpGetJson[Vector[ProductJson], String](
          s"$CoinbaseBaseRestEndpoint/products"
        ) map {
          case Left(products: Vector[ProductJson]) => products
            .filter(e => e.status == "online" && !e.trading_disabled && !e.cancel_only && !e.post_only)
            .map(_.toCoinbaseTradePair)
            .filterNot(e => exchangeConfig.assetBlocklist.contains(e.baseAsset) || exchangeConfig.assetBlocklist.contains(e.quoteAsset))
          case Right(error) => throw new RuntimeException(s"query products failed with: $error")
        },
        globalConfig.httpTimeout).toSet

    tradePairs = coinbaseTradePairs.map(_.toTradePair)
  }

  override def preStart(): Unit = {
    pullTradePairs()
  }

  override def receive: Receive = {
    // @formatter:off
    case GetTradePairs()         => sender() ! TradePairs(tradePairs) // from exchange
    case GetCoinbaseTradePairs() => sender() ! coinbaseTradePairs // from BinancePublicDataChannel
    case Status.Failure(cause)   => log.error("received failure", cause)
    // @formatter:on
  }
}

// Unless otherwise specified, all timestamps from API are returned in ISO 8601 with microseconds
