package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class SecretsConfig(apiKey: String,
                         apiSecretKey: String)
case class ExchangeConfig(exchangeName: String,
                          tradingSecrets: SecretsConfig,
                          withdrawalSecrets: SecretsConfig,
                          assets: Set[String],
                          makerFee: Double,
                          takerFee: Double,
                          orderBooksEnabled: Boolean)
case class OrderBundleSafetyGuardConfig(maximumReasonableWinPerOrderBundleUSDT: Double,
                                        maxOrderLimitTickerVariance: Double,
                                        maxTickerAge: Duration)
case class TradeRoomConfig(extendedTickerExchanges: List[String],
                           orderBooksEnabled: Boolean,
                           internalCommunicationTimeout: Timeout,
                           statsInterval: Duration,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig
                          )
case class LiquidityManagerConfig(liquidityStoringAssets: List[Asset])

object AppConfig {
  private val tradeRoomConfig: Config = ConfigFactory.load().getConfig("trade-room")
  val tradeRoom: TradeRoomConfig = {
    TradeRoomConfig(
      tradeRoomConfig.getStringList("reference-ticker-exchanges").asScala.toList,
      tradeRoomConfig.getBoolean("order-books-enabled"),
      Timeout.create(tradeRoomConfig.getDuration("internal-communication-timeout")),
      tradeRoomConfig.getDuration("stats-interval"),
      tradeRoomConfig.getConfig("order-bundle-safety-guard") match {
        case c: Config => OrderBundleSafetyGuardConfig(
          c.getDouble("max-reasonable-win-per-order-bundle-usdt"),
          c.getDouble("max-order-limit-ticker-variance"),
          c.getDuration("max-ticker-age")
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
    secretsConfig(c.getConfig("secrets.trading")),
    secretsConfig(c.getConfig("secrets.withdrawal")),
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    c.getBoolean("order-books-enabled")
  )

  def exchange(name: String): ExchangeConfig = exchangeConfig(name, exchangesConfig.getConfig(name))

  private val liquidityManagerConfig: Config = tradeRoomConfig.getConfig("liquidity-manager")
  val liquidityManager: LiquidityManagerConfig = LiquidityManagerConfig(
    liquidityManagerConfig.getStringList("liquidity-storing-assets").asScala.map(e => Asset(e)).toList
  )

  def trader(name: String): Config = tradeRoomConfig.getConfig(s"trader.$name")
}

