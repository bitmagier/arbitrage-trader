package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class ExchangeConfig(exchangeName:String, assets: Set[String], makerFee: Double, takerFee: Double, httpTimeout: FiniteDuration)
case class TradeRoomConfig(maxDataAge: Duration,
                           cachedDataLifetime: Duration,
                           referenceTickerExchanges: Seq[String],
                           initTimeout: Timeout,
                           internalCommunicationTimeout: Timeout)

object StaticConfig {
  private val tradeRoomConfig: Config = ConfigFactory.load().getConfig("trade-room")
  val tradeRoom: TradeRoomConfig =
    TradeRoomConfig(
      tradeRoomConfig.getDuration("max-data-age"),
      tradeRoomConfig.getDuration("cached-data-lifetime"),
      tradeRoomConfig.getStringList("reference-ticker-exchanges").asScala,
      Timeout.create(tradeRoomConfig.getDuration("init-timeout")),
      Timeout.create(tradeRoomConfig.getDuration("internal-communication-timeout"))
    )

  private val exchangesConfig: Config = tradeRoomConfig.getConfig("exchange")

  def activeExchanges: Seq[String] = exchangesConfig.getStringList("active").asScala

  private def exchangeConfig(name:String, c: Config) = ExchangeConfig(
    name,
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    FiniteDuration(c.getDuration("http-timeout").toNanos, TimeUnit.NANOSECONDS)
  )

  def exchange(name: String): ExchangeConfig = exchangeConfig(name, exchangesConfig.getConfig(name))

  def trader(name: String): Config = tradeRoomConfig.getConfig(s"trader.$name")
}

