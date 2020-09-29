package org.purevalue.arbitrage.util

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

import org.purevalue.arbitrage.traderoom.CryptoValue

object Util {
  def applyBalanceDiff(balance: Iterable[CryptoValue], diff: Iterable[CryptoValue]): Iterable[CryptoValue] = {
    balance.map {
      v => CryptoValue(v.asset, v.amount + diff.filter(_.asset == v.asset).map(_.amount).sum)
    } ++ diff.filter(e => !balance.exists(_.asset == e.asset))
  }


  def formatDecimal(d: Double): String = formatDecimal(d, 8)

  def formatDecimal(d: Double, visibleFractionDigits: Int, minimumFractionDigits: Int = 2): String = {
    val format = new DecimalFormat()
    format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.ENGLISH))
    format.setMinimumIntegerDigits(1)
    format.setMinimumFractionDigits(minimumFractionDigits)
    format.setMaximumFractionDigits(visibleFractionDigits)
    format.setGroupingUsed(false)
    format.format(d)
  }

  // format with exactly the given precision
  def formatDecimalWithFixPrecision(d: Double, precision: Int): String = {
    val numWholeDigits = d.toInt.toString.length
    val numFractionDigits: Int = precision - numWholeDigits
    formatDecimal(d, numFractionDigits, numFractionDigits)
  }

  def alignToStepSizeCeil(amount: Double, stepSize: Double): Double = {
    stepSize * (amount / stepSize).ceil
  }

  def alignToStepSizeNearest(price: Double, tickSize: Double): Double = {
    if (tickSize == 0.0) price
    else tickSize * (price / tickSize).round
  }

  def convertBytesToLowerCaseHex(bytes: Seq[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    sb.toString
  }
}
