package org.purevalue.arbitrage.util

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

object Util {

  def formatDecimal(d: Double): String = formatDecimal(d, 8)

  def formatDecimal(d: Double, visibleFractionDigits:Int, minimumFractionDigits:Int = 2): String = {
    val format = new DecimalFormat()
    format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.ENGLISH))
    format.setMinimumIntegerDigits(1)
    format.setMinimumFractionDigits(minimumFractionDigits)
    format.setMaximumFractionDigits(visibleFractionDigits)
    format.setGroupingUsed(false)
    format.format(d)
  }

  def formatDecimalWithPrecision(d: Double, precision:Int): String = {
    val numWholeDigits = d.toInt.toString.length
    val numFractionDigits: Int = precision - numWholeDigits
    formatDecimal(d, numFractionDigits, numFractionDigits)
  }

  def convertBytesToLowerCaseHex(bytes: Seq[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    sb.toString
  }
}
