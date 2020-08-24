package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class SecretsConfig(apiKey: String,
                         apiSecretKey: String)
case class ExchangeConfig(exchangeName: String,
                          secrets: SecretsConfig,
                          assets: Set[String],
                          makerFee: Double,
                          takerFee: Double,
                          orderBooksEnabled: Boolean)
case class OrderBundleSafetyGuardConfig(maximumReasonableWinPerOrderBundleUSDT: Double,
                                        maxOrderLimitTickerVariance: Double,
                                        maxTickerAge: Duration,
                                        minTotalGainInUSDT: Double)
case class TradeRoomConfig(extendedTickerExchanges: List[String],
                           orderBooksEnabled: Boolean,
                           internalCommunicationTimeout: Timeout,
                           gracefulStopTimeout: Timeout,
                           statsInterval: Duration,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig)
case class LiquidityManagerConfig(reserveAssets: List[Asset])

object AppConfig {
  private val log = LoggerFactory.getLogger("AppConfig")
  private val config = ConfigFactory.load()
  private val tradeRoomConfig: Config = config.getConfig("trade-room")
  val tradeRoom: TradeRoomConfig = {
    TradeRoomConfig(
      tradeRoomConfig.getStringList("reference-ticker-exchanges").asScala.toList,
      tradeRoomConfig.getBoolean("order-books-enabled"),
      Timeout.create(tradeRoomConfig.getDuration("internal-communication-timeout")), // TODO move to root-level
      Timeout.create(tradeRoomConfig.getDuration("graceful-stop-timeout")),
      tradeRoomConfig.getDuration("stats-interval"),
      tradeRoomConfig.getConfig("order-bundle-safety-guard") match {
        case c: Config => OrderBundleSafetyGuardConfig(
          c.getDouble("max-reasonable-win-per-order-bundle-usdt"),
          c.getDouble("max-order-limit-ticker-variance"),
          c.getDuration("max-ticker-age"),
          c.getDouble("min-total-gain-in-usdt")
        )
      }
    )
  }

  private val exchangesConfig: Config = tradeRoomConfig.getConfig("exchange")
  val activeExchanges: Seq[String] = exchangesConfig.getStringList("active").asScala
  val dataManagerInitTimeout: Duration = exchangesConfig.getDuration("data-manager-init-timeout")
  val httpTimeout: FiniteDuration = FiniteDuration(exchangesConfig.getDuration("http-timeout").toMillis, TimeUnit.MILLISECONDS)

  private def secretsConfig(c:Config) = SecretsConfig(
    c.getString("api-key"),
    c.getString("api-secret-key")
  )
  private def exchangeConfig(name: String, c: Config) = ExchangeConfig(
    name,
    secretsConfig(c.getConfig("secrets")),
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    c.getBoolean("order-books-enabled")
  )

  def exchange(name: String): ExchangeConfig = exchangeConfig(name, exchangesConfig.getConfig(name))

  private val liquidityManagerConfig: Config = config.getConfig("liquidity-manager")
  val liquidityManager: LiquidityManagerConfig = LiquidityManagerConfig(
    liquidityManagerConfig.getStringList("reserve-assets").asScala.map(e => Asset(e)).toList
  )

  def trader(name: String): Config = config.getConfig(s"trader.$name")

  log.info(s"Reserve Assets: ${liquidityManager.reserveAssets}")
}


