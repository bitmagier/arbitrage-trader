package org.purevalue.arbitrage

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

object Utils {
  def formatDecimal(d: Double): String = formatDecimal(d, 8)
  def formatDecimal(d: Double, visibleFractionDigits:Int): String = {
    val format = new DecimalFormat()
    format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.ENGLISH))
    format.setMinimumIntegerDigits(1)
    format.setMinimumFractionDigits(2)
    format.setMaximumFractionDigits(visibleFractionDigits)
    format.setGroupingUsed(false)
    format.format(d)
  }

  def convertBytesToLowerCaseHex(bytes: Seq[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    sb.toString
  }
}
