package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class SecretsConfig(apiKey: String,
                         apiSecretKey: String)

case class ExchangeConfig(exchangeName: String,
                          secrets: SecretsConfig,
                          reserveAssets: List[Asset], // reserve assets in order of importance; first in list is the primary reserve asset
                          tradeAssets: Set[String],
                          makerFee: Double,
                          takerFee: Double,
                          orderBooksEnabled: Boolean,
                          doNotTouchTheseAssets: Set[Asset]) {
  def fee: Fee = Fee(exchangeName, makerFee, takerFee)
}

object ExchangeConfig {
  def apply(name:String, c:com.typesafe.config.Config): ExchangeConfig = {
    val doNotTouchTheseAssets = c.getStringList("do-not-touch-these-assets").asScala.map(e => Asset(e)).toSet
    val reserveAssets = c.getStringList("reserve-assets").asScala.map(e => Asset(e)).toList
    if (reserveAssets.exists(doNotTouchTheseAssets.contains))
      throw new IllegalArgumentException(s"$name: reserve-assets & do-not-touch-these-assets overlap!")
    ExchangeConfig(
      name,
      secretsConfig(c.getConfig("secrets")),
      reserveAssets,
      c.getStringList("trade-assets").asScala.toSet,
      c.getDouble("fee.maker"),
      c.getDouble("fee.taker"),
      c.getBoolean("order-books-enabled"),
      doNotTouchTheseAssets
    )
  }

  private def secretsConfig(c: com.typesafe.config.Config) = SecretsConfig(
    c.getString("api-key"),
    c.getString("api-secret-key")
  )
}

case class OrderBundleSafetyGuardConfig(maximumReasonableWinPerOrderBundleUSDT: Double,
                                        maxOrderLimitTickerVariance: Double,
                                        maxTickerAge: Duration,
                                        minTotalGainInUSDT: Double)
case class TradeRoomConfig(productionMode: Boolean,
                           tradeSimulation: Boolean,
                           referenceTickerExchange: String,
                           orderBooksEnabled: Boolean,
                           stats: TradeRoomStatsConfig,
                           restartWhenAnExchangeDataStreamIsOlderThan: Duration,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig)
object TradeRoomConfig {
  def apply(c: com.typesafe.config.Config): TradeRoomConfig = TradeRoomConfig(
    c.getBoolean("production-mode"),
    c.getBoolean("trade-simulation"),
    c.getString("reference-ticker-exchange"),
    c.getBoolean("order-books-enabled"),
    TradeRoomStatsConfig(
      c.getDuration("stats.report-interval"),
      Asset(c.getString("stats.aggregated-liquidity-report-asset"))
    ),
    c.getDuration("restart-when-an-exchange-data-stream-is-older-than"),
    c.getConfig("order-bundle-safety-guard") match {
      case c: com.typesafe.config.Config => OrderBundleSafetyGuardConfig(
        c.getDouble("max-reasonable-win-per-order-bundle-usdt"),
        c.getDouble("max-order-limit-ticker-variance"),
        c.getDuration("max-ticker-age"),
        c.getDouble("min-total-gain-in-usdt")
      )
    })
}

case class TradeRoomStatsConfig(reportInterval: Duration,
                                aggregatedliquidityReportAsset: Asset)

case class LiquidityManagerConfig(liquidityLockMaxLifetime: Duration, // when a liquidity lock is not cleared, this is the maximum time, it can stay active
                                  liquidityDemandActiveTime: Duration, // when demanded liquidity is not requested within that time, the coins are transferred back to a reserve asset
                                  providingLiquidityExtra: Double, // we provide a little bit more than it was demanded, to potentially fulfill order requests with a slightly different amount
                                  maxAcceptableLocalTickerLossFromReferenceTicker: Double, // defines the maximum acceptable relative loss (local ticker versus reference ticker) for liquidity conversion transactions
                                  minimumKeepReserveLiquidityPerAssetInUSDT: Double, // when we convert to a reserve liquidity or re-balance our reserve liquidity, each of them should reach at least that value (measured in USDT)
                                  txLimitBelowOrAboveBestBidOrAsk: Double, // defines the rate we set our limit above the highest ask or below the lowest bid (use 0.0 for using exactly the bid or ask price).
                                  rebalanceTxGranularityInUSDT: Double) // that's the granularity (and also minimum amount) we transfer for reserve asset re-balance orders)}
object LiquidityManagerConfig {
  def apply(c: com.typesafe.config.Config): LiquidityManagerConfig = LiquidityManagerConfig(
    c.getDuration("liquidity-lock-max-lifetime"),
    c.getDuration("liquidity-demand-active-time"),
    c.getDouble("providing-liquidity-extra"),
    c.getDouble("max-acceptable-local-ticker-loss-from-reference-ticker"),
    c.getDouble("minimum-keep-reserve-liquidity-per-asset-in-usdt"),
    c.getDouble("tx-limit-below-or-above-best-bid-or-ask"),
    c.getDouble("rebalance-tx-granularity-in-usdt"))
}

object Config {
  private val c = ConfigFactory.load()

  val httpTimeout: FiniteDuration = FiniteDuration(c.getDuration("http-timeout").toMillis, TimeUnit.MILLISECONDS)
  val internalCommunicationTimeout: Timeout = Timeout.create(c.getDuration("internal-communication-timeout"))
  val internalCommunicationTimeoutWhileInit: Timeout = Timeout.create(c.getDuration("internal-communication-timeout-during-init"))
  val gracefulStopTimeout: Timeout = Timeout.create(c.getDuration("graceful-stop-timeout"))

  val tradeRoom: TradeRoomConfig = TradeRoomConfig(c.getConfig("trade-room"))

  private val exchangesConfig = c.getConfig("trade-room.exchange")
  val activeExchanges: Seq[String] = exchangesConfig.getStringList("active").asScala
  val dataManagerInitTimeout: Duration = exchangesConfig.getDuration("data-manager-init-timeout")

  def exchange(name: String): ExchangeConfig = ExchangeConfig(name, exchangesConfig.getConfig(name))

  val liquidityManager: LiquidityManagerConfig = LiquidityManagerConfig(c.getConfig("liquidity-manager"))

  def trader(name: String): com.typesafe.config.Config = c.getConfig(s"trader.$name")
}
