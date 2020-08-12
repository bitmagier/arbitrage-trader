package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.purevalue.arbitrage.AppConfig.tradeRoomConfig

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class ExchangeConfig(exchangeName: String,
                          assets: Set[String],
                          makerFee: Double,
                          takerFee: Double,
                          httpTimeout: FiniteDuration,
                          orderBooksEnabled: Boolean)
case class OrderBundleValidityGuardConfig(maximumReasonableWinPerOrderBundleUSDT: Double,
                                          maxOrderLimitTickerVariance: Double,
                                          maxTickerAge: Duration)
case class TradeRoomConfig(extendedTickerExchanges: List[String],
                           orderBooksEnabled: Boolean,
                           internalCommunicationTimeout: Timeout,
                           statsInterval: Duration,
                           orderBundleValidityGuard: OrderBundleValidityGuardConfig
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
      tradeRoomConfig.getConfig("order-bundle-validity-guard") match {
        case c: Config => OrderBundleValidityGuardConfig(
          c.getDouble("max-reasonable-win-per-order-bundle-usdt"),
          c.getDouble("max-order-limit-ticker-variance"),
          c.getDuration("max-ticker-age")
        )
      }
    )
  }

  def activeExchanges: Seq[String] = exchangesConfig.getStringList("active").asScala

  def dataManagerInitTimeout: Duration = exchangesConfig.getDuration("data-manager-init-timeout")

  private val liquidityManagerConfig: Config = tradeRoomConfig.getConfig("liquidity-manager")
  val liquidityManager: LiquidityManagerConfig = LiquidityManagerConfig(
    liquidityManagerConfig.getStringList("liquidity-storing-assets").asScala.map(e => Asset(e)).toList
  )

  private val exchangesConfig: Config = tradeRoomConfig.getConfig("exchange")

  private def exchangeConfig(name: String, c: Config) = ExchangeConfig(
    name,
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    FiniteDuration(c.getDuration("http-timeout").toNanos, TimeUnit.NANOSECONDS),
    c.getBoolean("order-books-enabled")
  )

  def exchange(name: String): ExchangeConfig = exchangeConfig(name, exchangesConfig.getConfig(name))

  def trader(name: String): Config = tradeRoomConfig.getConfig(s"trader.$name")
}
