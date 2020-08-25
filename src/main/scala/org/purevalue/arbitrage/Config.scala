package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
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
                           statsInterval: Duration,
                           restartWhenASingleTickerIsOlderThan: Duration,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig)
case class LiquidityManagerConfig(reserveAssets: List[Asset])

object Config {
  private val log = LoggerFactory.getLogger("AppConfig")
  private val config = ConfigFactory.load()

  val httpTimeout: FiniteDuration = FiniteDuration(config.getDuration("http-timeout").toMillis, TimeUnit.MILLISECONDS)
  val internalCommunicationTimeout: Timeout = Timeout.create(config.getDuration("internal-communication-timeout"))
  val internalCommunicationTimeoutWhileInit: Timeout = Timeout.create(config.getDuration("internal-communication-timeout-during-init"))
  val gracefulStopTimeout: Timeout = Timeout.create(config.getDuration("graceful-stop-timeout"))

  private val tradeRoomConfig = config.getConfig("trade-room")
  val tradeRoom: TradeRoomConfig = {
    TradeRoomConfig(
      tradeRoomConfig.getStringList("reference-ticker-exchanges").asScala.toList,
      tradeRoomConfig.getBoolean("order-books-enabled"),
      tradeRoomConfig.getDuration("stats-interval"),
      tradeRoomConfig.getDuration("restart-when-a-single-ticker-is-older-than"),
      tradeRoomConfig.getConfig("order-bundle-safety-guard") match {
        case c: com.typesafe.config.Config => OrderBundleSafetyGuardConfig(
          c.getDouble("max-reasonable-win-per-order-bundle-usdt"),
          c.getDouble("max-order-limit-ticker-variance"),
          c.getDuration("max-ticker-age"),
          c.getDouble("min-total-gain-in-usdt")
        )
      }
    )
  }

  private val exchangesConfig = tradeRoomConfig.getConfig("exchange")
  val activeExchanges: Seq[String] = exchangesConfig.getStringList("active").asScala
  val dataManagerInitTimeout: Duration = exchangesConfig.getDuration("data-manager-init-timeout")

  private def secretsConfig(c: com.typesafe.config.Config) = SecretsConfig(
    c.getString("api-key"),
    c.getString("api-secret-key")
  )

  private def exchangeConfig(name: String, c: com.typesafe.config.Config) = ExchangeConfig(
    name,
    secretsConfig(c.getConfig("secrets")),
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    c.getBoolean("order-books-enabled")
  )

  def exchange(name: String): ExchangeConfig = exchangeConfig(name, exchangesConfig.getConfig(name))

  private val liquidityManagerConfig = config.getConfig("liquidity-manager")
  val liquidityManager: LiquidityManagerConfig = LiquidityManagerConfig(
    liquidityManagerConfig.getStringList("reserve-assets").asScala.map(e => Asset(e)).toList
  )

  def trader(name: String): com.typesafe.config.Config = config.getConfig(s"trader.$name")

  log.info(s"Reserve Assets: ${liquidityManager.reserveAssets}")
}
