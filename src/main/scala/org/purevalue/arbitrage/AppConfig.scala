package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class ExchangeConfig(exchangeName:String,
                          assets: Set[String],
                          makerFee: Double,
                          takerFee: Double,
                          httpTimeout: FiniteDuration,
                          orderBooksEnabled:Boolean)
case class TradeRoomConfig(extendedTickerExchanges: Seq[String],
                           orderBooksEnabled: Boolean,
                           internalCommunicationTimeout: Timeout,
                           statsInterval: Duration,
                           maximumReasonableWinPerOrderBundleUSDT: Double,
                           maxOrderLimitTickerVariance: Double)

object AppConfig {
  private val tradeRoomConfig: Config = ConfigFactory.load().getConfig("trade-room")
  val tradeRoom: TradeRoomConfig =
    TradeRoomConfig(
      tradeRoomConfig.getStringList("extended-ticker-exchanges").asScala,
      tradeRoomConfig.getBoolean("order-books-enabled"),
      Timeout.create(tradeRoomConfig.getDuration("internal-communication-timeout")),
      tradeRoomConfig.getDuration("stats-interval"),
      tradeRoomConfig.getDouble("order-validity-check.max-reasonable-win-per-order-bundle-usdt"),
      tradeRoomConfig.getDouble("order-validity-check.max-order-limit-ticker-variance")
    )

  private val exchangesConfig: Config = tradeRoomConfig.getConfig("exchange")

  def activeExchanges: Seq[String] = exchangesConfig.getStringList("active").asScala
  def dataManagerInitTimeout: Duration = exchangesConfig.getDuration("data-manager-init-timeout")

  private def exchangeConfig(name:String, c: Config) = ExchangeConfig(
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
