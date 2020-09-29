package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.purevalue.arbitrage.adapter.Fee
import org.purevalue.arbitrage.traderoom.Asset

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class SecretsConfig(apiKey: String,
                         apiSecretKey: String,
                         apiKeyPassphrase: Option[String])

case class ExchangeConfig(exchangeName: String,
                          secrets: SecretsConfig,
                          reserveAssets: List[Asset], // reserve assets in order of importance; first in list is the primary reserve asset
                          assetBlocklist: Set[Asset],
                          makerFee: Double,
                          takerFee: Double,
                          doNotTouchTheseAssets: Seq[Asset],
                          refCode: Option[String]) {
  def fee: Fee = Fee(exchangeName, makerFee, takerFee)
}

object ExchangeConfig {
  def apply(name: String, c: com.typesafe.config.Config): ExchangeConfig = {
    val doNotTouchTheseAssets = c.getStringList("do-not-touch-these-assets").asScala.map(e => Asset(e))
    if (doNotTouchTheseAssets.exists(_.isFiat)) throw new IllegalArgumentException(s"$name: Don't worry bro, I'll never touch Fiat Money")
    val reserveAssets = c.getStringList("reserve-assets").asScala.map(e => Asset(e)).toList
    if (reserveAssets.exists(doNotTouchTheseAssets.contains)) throw new IllegalArgumentException(s"$name: reserve-assets & do-not-touch-these-assets overlap!")
    if (reserveAssets.exists(_.isFiat)) throw new IllegalArgumentException(s"$name: Cannot us a Fiat currency as reserve-asset")

    ExchangeConfig(
      name,
      secretsConfig(c.getConfig("secrets")),
      reserveAssets,
      c.getStringList("assets-blocklist").asScala.map(e => Asset(e)).toSet,
      c.getDouble("fee.maker"),
      c.getDouble("fee.taker"),
      doNotTouchTheseAssets,
      if (c.hasPath("ref-code")) Some(c.getString("ref-code")) else None,
    )
  }

  private def secretsConfig(c: com.typesafe.config.Config) = SecretsConfig(
    c.getString("api-key"),
    c.getString("api-secret-key"),
    if (c.getConfig("api-key-passphrase").isEmpty) None else Some(c.getString("api-key-passphrase"))
  )
}

case class OrderBundleSafetyGuardConfig(maximumReasonableWinPerOrderBundleUSDT: Double,
                                        maxOrderLimitTickerVariance: Double,
                                        maxTickerAge: Duration,
                                        minTotalGainInUSDT: Double,
                                        txLimitAwayFromEdgeLimit: Double)
object OrderBundleSafetyGuardConfig {
  def apply(c: com.typesafe.config.Config): OrderBundleSafetyGuardConfig = OrderBundleSafetyGuardConfig(
    c.getDouble("max-reasonable-win-per-order-bundle-usdt"),
    c.getDouble("max-order-limit-ticker-variance"),
    c.getDuration("max-ticker-age"),
    c.getDouble("min-total-gain-in-usdt"),
    c.getDouble("tx-limit-away-from-edge-limit")
  )
}
case class TradeRoomConfig(tradeSimulation: Boolean,
                           referenceTickerExchange: String,
                           maxOrderLifetime: Duration,
                           restarExchangeWhenDataStreamIsOlderThan: Duration,
                           pioneerOrderValueUSDT: Double,
                           dataManagerInitTimeout: Duration,
                           stats: TradeRoomStatsConfig,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig,
                           liquidityManager: LiquidityManagerConfig,
                           exchanges: Map[String, ExchangeConfig]) {
  if (pioneerOrderValueUSDT > 100.0) throw new IllegalArgumentException("pioneer order value is unnecessary big")
  if (!exchanges.contains(referenceTickerExchange)) throw new IllegalArgumentException("reference-ticker-exchange not in list of active exchanges")
}
object TradeRoomConfig {
  def apply(c: com.typesafe.config.Config): TradeRoomConfig = TradeRoomConfig(
    c.getBoolean("trade-simulation"),
    c.getString("reference-ticker-exchange"),
    c.getDuration("max-order-lifetime"),
    c.getDuration("restart-exchange-when-data-stream-is-older-than"),
    c.getDouble("pioneer-order-value-usdt"),
    c.getDuration("data-manager-init-timeout"),
    TradeRoomStatsConfig(
      c.getDuration("stats.report-interval"),
      Asset(c.getString("stats.aggregated-liquidity-report-asset"))
    ),
    OrderBundleSafetyGuardConfig(c.getConfig("order-bundle-safety-guard")),
    LiquidityManagerConfig(c.getConfig("liquidity-manager")),
    c.getStringList("active-exchanges").asScala.map(e => e -> ExchangeConfig(e, c.getConfig(s"exchange.$e"))).toMap
  )
}

case class TradeRoomStatsConfig(reportInterval: Duration,
                                aggregatedliquidityReportAsset: Asset)

case class LiquidityManagerConfig(liquidityLockMaxLifetime: Duration, // when a liquidity lock is not cleared, this is the maximum time, it can stay active
                                  liquidityDemandActiveTime: Duration, // when demanded liquidity is not requested within that time, the coins are transferred back to a reserve asset
                                  providingLiquidityExtra: Double, // we provide a little bit more than it was demanded, to potentially fulfill order requests with a slightly different amount
                                  maxAcceptableExchangeRateLossVersusReferenceTicker: Double, // defines the maximum acceptable relative loss (local exchange rate versus reference ticker) for liquidity conversion transactions
                                  minimumKeepReserveLiquidityPerAssetInUSDT: Double, // when we convert to a reserve liquidity or re-balance our reserve liquidity, each of them should reach at least that value (measured in USDT)
                                  txLimitAwayFromEdgeLimit: Double,  // [limit-reality-adjustment-rate] defines the rate we set our limit above the highest ask or below the lowest bid (use 0.0 for matching exactly the bid or ask price).
                                  rebalanceTxGranularityInUSDT: Double, // that's the granularity (and also minimum amount) we transfer for reserve asset re-balance orders)}
                                  dustLevelInUsdt: Double) // we don't try to convert back assets with a value below that one back to a reserve asset
object LiquidityManagerConfig {
  def apply(c: com.typesafe.config.Config): LiquidityManagerConfig = LiquidityManagerConfig(
    c.getDuration("liquidity-lock-max-lifetime"),
    c.getDuration("liquidity-demand-active-time"),
    c.getDouble("providing-liquidity-extra"),
    c.getDouble("max-acceptable-exchange-rate-loss-versus-reference-ticker"),
    c.getDouble("minimum-keep-reserve-liquidity-per-asset-in-usdt"),
    c.getDouble("tx-limit-away-from-edge-limit"),
    c.getDouble("rebalance-tx-granularity-in-usdt"),
    c.getDouble("dust-level-in-usdt")
  )
}

case class GlobalConfig(httpTimeout: FiniteDuration,
                        internalCommunicationTimeout: Timeout,
                        internalCommunicationTimeoutDuringInit: Timeout,
                        gracefulShutdownTimeout: Duration)
object GlobalConfig {
  def apply(c: com.typesafe.config.Config): GlobalConfig = GlobalConfig(
    FiniteDuration(c.getDuration("http-timeout").toMillis, TimeUnit.MILLISECONDS),
    Timeout.create(c.getDuration("internal-communication-timeout")),
    Timeout.create(c.getDuration("internal-communication-timeout-during-init")),
    c.getDuration("graceful-shutdown-timeout")
  )
}

case class Config(c: com.typesafe.config.Config,
                  global: GlobalConfig,
                  tradeRoom: TradeRoomConfig) {
  def trader(name: String): com.typesafe.config.Config = c.getConfig(s"trader.$name")
}
object Config {
  def load(): Config = {
    val c = ConfigFactory.load()
    val globalConfig = GlobalConfig(c.getConfig("global"))
    val tradeRoom = TradeRoomConfig(c.getConfig("trade-room"))
    Config(c, globalConfig, tradeRoom)
  }
}

