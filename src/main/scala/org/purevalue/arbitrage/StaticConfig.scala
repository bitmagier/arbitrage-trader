package org.purevalue.arbitrage

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class ExchangeConfig(assets: Set[String], makerFee: Double, takerFee: Double, httpTimeout: FiniteDuration)
case class TradeRoomConfig(maxDataAge: Duration, referenceTickerExchanges: Seq[String])

object StaticConfig {
  private val config: Config = ConfigFactory.load()

  def activeExchanges: Seq[String] = config.getStringList("exchange.active").asScala

  private def exchangeConfig(c: Config) = ExchangeConfig(
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    FiniteDuration(c.getDuration("http-timeout").toNanos, TimeUnit.NANOSECONDS)
  )

  def exchange(name: String): ExchangeConfig = exchangeConfig(config.getConfig(s"exchange.$name"))

  def trader(name: String): Config = config.getConfig(s"trader.$name")

  val tradeRoom: TradeRoomConfig =
    TradeRoomConfig(
      config.getDuration("trade-room.max-data-age"),
      config.getStringList("trade-room.reference-ticker-exchanges").asScala
    )
}

