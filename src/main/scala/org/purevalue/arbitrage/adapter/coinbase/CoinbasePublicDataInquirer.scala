package org.purevalue.arbitrage.adapter.coinbase

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.purevalue.arbitrage.adapter.PublicDataInquirer
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataInquirer.{CoinbaseBaseRestEndpoint, GetCoinbaseTradePairs}
import org.purevalue.arbitrage.traderoom.{Asset, TradePair}
import org.purevalue.arbitrage.util.HttpUtil
import org.purevalue.arbitrage.util.HttpUtil.httpGetJson
import org.purevalue.arbitrage.util.Util.stepSizeToFractionDigits
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

private[coinbase] case class CoinbaseTradePair(id: String, // = product_id
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

private[coinbase] case class CurrencyJson(id: String,
                                          name: String,
                                          min_size: String)

private[coinbase] object CoinbaseJsonProtocol extends DefaultJsonProtocol {
  implicit val productJson: RootJsonFormat[ProductJson] = jsonFormat13(ProductJson)
  implicit val currencyJson: RootJsonFormat[CurrencyJson] = jsonFormat3(CurrencyJson)
}

object CoinbasePublicDataInquirer {
  def apply(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig):
  Behavior[PublicDataInquirer.Command] =
    Behaviors.setup(context => new CoinbasePublicDataInquirer(context, globalConfig, exchangeConfig))

  val CoinbaseBaseRestEndpoint: String = "https://api.pro.coinbase.com" // "https://api-public.sandbox.pro.coinbase.com" //

  case class GetCoinbaseTradePairs(replyTo: ActorRef[Set[CoinbaseTradePair]]) extends PublicDataInquirer.Command
}
private[coinbase] class CoinbasePublicDataInquirer(context: ActorContext[PublicDataInquirer.Command],
                                                   globalConfig: GlobalConfig,
                                                   exchangeConfig: ExchangeConfig) extends PublicDataInquirer(context) {

  import PublicDataInquirer._

  var tradePairs: Set[TradePair] = _
  var coinbaseTradePairs: Set[CoinbaseTradePair] = _

  import CoinbaseJsonProtocol._

  def registerAssets(): Unit = {
    Await.result(
      httpGetJson[Seq[CurrencyJson], String](s"$CoinbaseBaseRestEndpoint/currencies"),
      globalConfig.httpTimeout.plus(1.second)) match {
      case Left(currencies) =>
        // we don't have information about what is FIAT here, but Asset-register will correct that
        currencies.foreach { e =>
          Asset.register(e.id, Some(e.name), None, stepSizeToFractionDigits(e.min_size.toDouble), exchangeConfig.assetSourceWeight)
        }
      case Right(error) =>
        context.log.error(s"coinbase: GET /currencies failed: $error")
        throw new RuntimeException()
    }
  }

  def pullTradePairs(): Unit = {
    coinbaseTradePairs =
      Await.result(
        HttpUtil.httpGetJson[Vector[ProductJson], String](
          s"$CoinbaseBaseRestEndpoint/products"
        ) map {
          case Left(products: Vector[ProductJson]) =>
            if (context.log.isDebugEnabled) context.log.debug(s"""${products.mkString("\n")}""")
            products
              .filter(e => e.status == "online" && !e.trading_disabled && !e.cancel_only && !e.post_only)
              .map(_.toCoinbaseTradePair)
              .filterNot(e => exchangeConfig.assetBlocklist.contains(e.baseAsset) || exchangeConfig.assetBlocklist.contains(e.quoteAsset))
          case Right(error) =>
            context.log.error(s"query products failed with: $error")
            throw new RuntimeException()
        },
        globalConfig.httpTimeout).toSet

    tradePairs = coinbaseTradePairs.map(_.toTradePair)
  }

  def init(): Unit = {
    registerAssets()
    pullTradePairs()
  }

  override def onMessage(message: Command): Behavior[Command] = {
    message match {
      // @formatter:off
      case GetAllTradePairs(replyTo)      => replyTo ! tradePairs
      case GetCoinbaseTradePairs(replyTo) => replyTo ! coinbaseTradePairs
      // @formatter:on
    }
    this
  }

  init()
}

// Unless otherwise specified, all timestamps from API are returned in ISO 8601 with microseconds
