package org.purevalue.arbitrage

import java.text.DecimalFormat

object Utils {
  def formatDecimal(d: Double): String = new DecimalFormat("#.##########").format(d)

  def convertBytesToLowerCaseHex(bytes: Seq[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    sb.toString
  }
}
