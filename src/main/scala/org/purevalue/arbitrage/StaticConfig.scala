package org.purevalue.arbitrage

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

case class ExchangeConfig(assets:Set[String], makerFee:Double, takerFee:Double)
object StaticConfig {
  private val config: Config = ConfigFactory.load()
  def activeExchanges: Seq[String] = config.getStringList("exchanges.active").asScala

  private def exchangeConfig(c: Config) = ExchangeConfig(c.getStringList("assets").asScala.toSet, c.getDouble("fee.maker"), c.getDouble("fee.taker"))
  def exchange(name:String):ExchangeConfig = exchangeConfig(config.getConfig(s"exchanges.$name"))
}

