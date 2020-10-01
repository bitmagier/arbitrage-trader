package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.purevalue.arbitrage.traderoom.Asset

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class SecretsConfig(apiKey: String,
                         apiSecretKey: String,
                         apiKeyPassphrase: Option[String])

case class ExchangeConfig(name: String,
                          reserveAssets: List[Asset], // reserve assets in order of importance; first in list is the primary reserve asset
                          assetBlocklist: Set[Asset],
                          usdEquivalentCoin: Asset, // primary local USD equivalent coin. USDT, USDC etc. for amount calculations
                          feeRate: Double, // average rate
                          doNotTouchTheseAssets: Seq[Asset],
                          secrets: SecretsConfig,
                          refCode: Option[String],
                          assetSourceWeight: Int) {
}

object ExchangeConfig {
  def apply(name: String, c: com.typesafe.config.Config): ExchangeConfig = {
    val rawDoNotTouchAssets = c.getStringList("do-not-touch-these-assets").asScala
    rawDoNotTouchAssets.foreach(Asset.register(_, None, None))
    val doNotTouchTheseAssets = rawDoNotTouchAssets.map(e => Asset(e))
    if (doNotTouchTheseAssets.exists(_.isFiat)) throw new IllegalArgumentException(s"$name: Don't worry bro, I'll never touch Fiat Money")
    val rawReserveAssets = c.getStringList("reserve-assets").asScala
    rawReserveAssets.foreach(Asset.register(_, None, None))
    val reserveAssets = rawReserveAssets.map(e => Asset(e)).toList
    if (reserveAssets.exists(doNotTouchTheseAssets.contains)) throw new IllegalArgumentException(s"$name: reserve-assets & do-not-touch-these-assets overlap!")
    if (reserveAssets.exists(_.isFiat)) throw new IllegalArgumentException(s"$name: Cannot us a Fiat currency as reserve-asset")
    if (reserveAssets.size < 2) throw new IllegalArgumentException(s"$name: Need at least two reserve assets (more are better)")

    val assetBlocklist = c.getStringList("assets-blocklist").asScala
    assetBlocklist.foreach(Asset.register(_, None, None))

    val usdEquivalentCoin = c.getString("usd-equivalent-coin")
    Asset.register(usdEquivalentCoin, None, None)

    ExchangeConfig(
      name,
      reserveAssets,
      assetBlocklist.map(e => Asset(e)).toSet,
      Asset(usdEquivalentCoin),
      c.getDouble("fee-rate"),
      doNotTouchTheseAssets,
      secretsConfig(c.getConfig("secrets")),
      if (c.hasPath("ref-code")) Some(c.getString("ref-code")) else None,
      c.getInt("asset-source-weight")
    )
  }

  private def secretsConfig(c: com.typesafe.config.Config) = SecretsConfig(
    c.getString("api-key"),
    c.getString("api-secret-key"),
    if (c.hasPath("api-key-passphrase")) Some(c.getString("api-key-passphrase")) else None
  )
}

case class OrderBundleSafetyGuardConfig(maximumReasonableWinPerOrderBundleUSD: Double,
                                        maxOrderLimitTickerVariance: Double,
                                        maxTickerAge: Duration,
                                        minTotalGainInUSD: Double,
                                        txLimitAwayFromEdgeLimit: Double)
object OrderBundleSafetyGuardConfig {
  def apply(c: com.typesafe.config.Config): OrderBundleSafetyGuardConfig = OrderBundleSafetyGuardConfig(
    c.getDouble("max-reasonable-win-per-order-bundle-usd"),
    c.getDouble("max-order-limit-ticker-variance"),
    c.getDuration("max-ticker-age"),
    c.getDouble("min-total-gain-in-usd"),
    c.getDouble("tx-limit-away-from-edge-limit")
  )
}
case class TradeRoomConfig(tradeSimulation: Boolean,
                           referenceTickerExchange: String,
                           maxOrderLifetime: Duration,
                           restarExchangeWhenDataStreamIsOlderThan: Duration,
                           pioneerOrderValueUSD: Double,
                           dataManagerInitTimeout: Duration,
                           statsReportInterval: Duration,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig,
                           liquidityManager: LiquidityManagerConfig,
                           exchanges: Map[String, ExchangeConfig]) {
  if (pioneerOrderValueUSD > 100.0) throw new IllegalArgumentException("pioneer order value is unnecessary big")
  if (!exchanges.contains(referenceTickerExchange)) throw new IllegalArgumentException("reference-ticker-exchange not in list of active exchanges")
}
object TradeRoomConfig {
  def apply(c: com.typesafe.config.Config): TradeRoomConfig = {
    TradeRoomConfig(
      c.getBoolean("trade-simulation"),
      c.getString("reference-ticker-exchange"),
      c.getDuration("max-order-lifetime"),
      c.getDuration("restart-exchange-when-data-stream-is-older-than"),
      c.getDouble("pioneer-order-value-usd"),
      c.getDuration("data-manager-init-timeout"),
      c.getDuration("stats-report-interval"),
      OrderBundleSafetyGuardConfig(c.getConfig("order-bundle-safety-guard")),
      LiquidityManagerConfig(c.getConfig("liquidity-manager")),
      c.getStringList("active-exchanges").asScala.map(e => e -> ExchangeConfig(e, c.getConfig(s"exchange.$e"))).toMap
    )
  }
}

case class LiquidityManagerConfig(liquidityLockMaxLifetime: Duration, // when a liquidity lock is not cleared, this is the maximum time, it can stay active
                                  liquidityDemandActiveTime: Duration, // when demanded liquidity is not requested within that time, the coins are transferred back to a reserve asset
                                  providingLiquidityExtra: Double, // we provide a little bit more than it was demanded, to potentially fulfill order requests with a slightly different amount
                                  maxAcceptableExchangeRateLossVersusReferenceTicker: Double, // defines the maximum acceptable relative loss (local exchange rate versus reference ticker) for liquidity conversion transactions
                                  minimumKeepReserveLiquidityPerAssetInUSD: Double, // when we convert to a reserve liquidity or re-balance our reserve liquidity, each of them should reach at least that value (measured in USD)
                                  txLimitAwayFromEdgeLimit: Double, // [limit-reality-adjustment-rate] defines the rate we set our limit above the highest ask or below the lowest bid (use 0.0 for matching exactly the bid or ask price).
                                  rebalanceTxGranularityInUSD: Double, // that's the granularity (and also minimum amount) we transfer for reserve asset re-balance orders)}
                                  dustLevelInUSD: Double) // we don't try to convert back assets with a value below that one back to a reserve asset
object LiquidityManagerConfig {
  def apply(c: com.typesafe.config.Config): LiquidityManagerConfig = LiquidityManagerConfig(
    c.getDuration("liquidity-lock-max-lifetime"),
    c.getDuration("liquidity-demand-active-time"),
    c.getDouble("providing-liquidity-extra"),
    c.getDouble("max-acceptable-exchange-rate-loss-versus-reference-ticker"),
    c.getDouble("minimum-keep-reserve-liquidity-per-asset-in-usd"),
    c.getDouble("tx-limit-away-from-edge-limit"),
    c.getDouble("rebalance-tx-granularity-in-usd"),
    c.getDouble("dust-level-in-usd")
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

