package org.purevalue.arbitrage

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class ExchangeConfig(assets:Set[String], makerFee:Double, takerFee:Double, httpTimeout:FiniteDuration)

object StaticConfig {
  private val config: Config = ConfigFactory.load()
  def activeExchanges: Seq[String] = config.getStringList("exchanges.active").asScala

  private def exchangeConfig(c: Config) = ExchangeConfig(
    c.getStringList("assets").asScala.toSet,
    c.getDouble("fee.maker"),
    c.getDouble("fee.taker"),
    FiniteDuration(c.getDuration("httpTimeout").toNanos, TimeUnit.NANOSECONDS)
  )
  def exchange(name:String):ExchangeConfig = exchangeConfig(config.getConfig(s"exchanges.$name"))
}

