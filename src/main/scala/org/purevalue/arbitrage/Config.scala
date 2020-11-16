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
                          deliversOrderBook: Boolean,
                          deliversStats24h: Boolean,
                          reserveAssets: List[Asset], // reserve assets in order of importance; first in list is the primary reserve asset
                          assetBlocklist: Set[Asset],
                          usdEquivalentCoin: Asset, // primary local USD equivalent coin. USDT, USDC etc. for amount calculations
                          feeRate: Double, // average rate
                          doNotTouchTheseAssets: Set[Asset],
                          tickerIsRealtime: Boolean, // whether we get a realtime ticker from that exchange or not
                          secrets: SecretsConfig,
                          refCode: Option[String],
                          assetSourceWeight: Int,
                          pullTradePairStatsInterval: Duration) {
  def primaryReserveAsset: Asset = reserveAssets.head
}

object ExchangeConfig {
  def apply(name: String, c: com.typesafe.config.Config): ExchangeConfig = {
    val rawDoNotTouchAssets = c.getStringList("do-not-touch-these-assets").asScala
    rawDoNotTouchAssets.foreach(Asset.register(_, None, None))
    val doNotTouchTheseAssets = rawDoNotTouchAssets.map(e => Asset(e)).toSet
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
      c.getBoolean("delivers-order-book"),
      c.getBoolean("delivers-stats24h"),
      reserveAssets,
      assetBlocklist.map(e => Asset(e)).toSet,
      Asset(usdEquivalentCoin),
      c.getDouble("fee-rate"),
      doNotTouchTheseAssets,
      c.getBoolean("ticker-is-realtime"),
      secretsConfig(c.getConfig("secrets")),
      if (c.hasPath("ref-code")) Some(c.getString("ref-code")) else None,
      c.getInt("asset-source-weight"),
      c.getDuration("pull-trade-pair-stats-interval")
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
                                        minTotalGainInUSD: Double)
object OrderBundleSafetyGuardConfig {
  def apply(c: com.typesafe.config.Config): OrderBundleSafetyGuardConfig = OrderBundleSafetyGuardConfig(
    c.getDouble("max-reasonable-win-per-order-bundle-usd"),
    c.getDouble("max-order-limit-ticker-variance"),
    c.getDuration("max-ticker-age"),
    c.getDouble("min-total-gain-in-usd")
  )
}
case class TradeRoomConfig(tradeSimulation: Boolean,
                           referenceTickerExchange: String,
                           maxOrderLifetime: Duration,
                           restarWhenDataStreamIsOlderThan: Duration,
                           pioneerOrderValueUSD: Double,
                           statsReportInterval: Duration,
                           traderTriggerInterval: Duration,
                           orderBundleSafetyGuard: OrderBundleSafetyGuardConfig,
                           activeExchanges: List[String]) {
  if (pioneerOrderValueUSD > 100.0) throw new IllegalArgumentException("pioneer order value is unnecessary big")
  if (!activeExchanges.contains(referenceTickerExchange)) throw new IllegalArgumentException("reference-ticker-exchange not in list of active exchanges")
}
object TradeRoomConfig {
  def apply(c: com.typesafe.config.Config): TradeRoomConfig = {
    TradeRoomConfig(
      c.getBoolean("trade-simulation"),
      c.getString("reference-ticker-exchange"),
      c.getDuration("max-order-lifetime"),
      c.getDuration("restart-when-data-stream-is-older-than"),
      c.getDouble("pioneer-order-value-usd"),
      c.getDuration("stats-report-interval"),
      c.getDuration("trader-trigger-interval"),
      OrderBundleSafetyGuardConfig(c.getConfig("order-bundle-safety-guard")),
      c.getStringList("active-exchanges").asScala.toList
    )
  }
}

case class LiquidityManagerConfig(liquidityLockMaxLifetime: Duration, // when a liquidity lock is not cleared, this is the maximum time, it can stay active
                                  liquidityDemandActiveTime: Duration, // when demanded liquidity is not requested within that time, the coins are transferred back to a reserve asset
                                  maxAcceptableExchangeRateLossVersusReferenceTicker: Double, // defines the maximum acceptable relative loss (local exchange rate versus reference ticker) for liquidity conversion transactions
                                  minimumKeepReserveLiquidityPerAssetInUSD: Double, // when we convert to a reserve liquidity or re-balance our reserve liquidity, each of them should reach at least that value (measured in USD)
                                  orderbookBasedTxLimitQuantityOverbooking: Double, // we assume, we have to trade more quantity (real quantity multiplied by this factor), when we calculate the ideal order limit based on an orderbook, because in reality we are not alone on the exchange
                                  tickerBasedTxLimitBeyondEdgeLimit: Double, // [limit-reality-adjustment-rate] for ticker based limit calculation; defines the rate we set our limit above the highest ask or below the lowest bid (use 0.0 for matching exactly the bid or ask price).
                                  txValueGranularityInUSD: Double) // that's the granularity (and also minimum amount) we transfer for reserve asset re-balance orders)}
object LiquidityManagerConfig {
  def apply(c: com.typesafe.config.Config): LiquidityManagerConfig = LiquidityManagerConfig(
    c.getDuration("liquidity-lock-max-lifetime"),
    c.getDuration("liquidity-demand-active-time"),
    c.getDouble("max-acceptable-exchange-rate-loss-versus-reference-ticker"),
    c.getDouble("minimum-keep-reserve-liquidity-per-asset-in-usd"),
    c.getDouble("orderbook-based-tx-limit-quantity-overbooking"),
    c.getDouble("ticker-based-tx-limit-beyond-edge-limit"),
    c.getDouble("tx-value-granularity-in-usd")
  )
}

case class GlobalConfig(httpTimeout: FiniteDuration,
                        internalCommunicationTimeout: Timeout,
                        internalCommunicationTimeoutDuringInit: Timeout)
object GlobalConfig {
  def apply(c: com.typesafe.config.Config): GlobalConfig = GlobalConfig(
    FiniteDuration(c.getDuration("http-timeout").toMillis, TimeUnit.MILLISECONDS),
    Timeout.create(c.getDuration("internal-communication-timeout")),
    Timeout.create(c.getDuration("internal-communication-timeout-during-init"))
  )
}

case class Config(global: GlobalConfig,
                  tradeRoom: TradeRoomConfig,
                  exchanges: Map[String, ExchangeConfig],
                  liquidityManager: LiquidityManagerConfig) {
}
object Config {
  private val c = ConfigFactory.load()

  def load(): Config = {
    val globalConfig = GlobalConfig(c.getConfig("global"))
    val tradeRoom = TradeRoomConfig(c.getConfig("trade-room"))
    val exchanges: Map[String, ExchangeConfig] = tradeRoom.activeExchanges
      .map(e => e -> ExchangeConfig(e, c.getConfig(s"exchange.$e"))).toMap
    val liquidityManager = LiquidityManagerConfig(c.getConfig("liquidity-manager"))

    if (!exchanges(tradeRoom.referenceTickerExchange).tickerIsRealtime) throw new IllegalArgumentException("reference ticker needs to be a realtime ticker")

    Config(globalConfig, tradeRoom, exchanges, liquidityManager)
  }

  def trader(name: String): com.typesafe.config.Config = c.getConfig(s"trader.$name")
}
