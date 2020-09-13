package org.purevalue.arbitrage.adapter.bitfinex

import java.text.ParseException


class BitfinexDataArrayMessageParser(val s:String) {
  var pos:Int = 0

  private def skipWhitespaces():Unit = {
    while (s.charAt(pos).isWhitespace) pos += 1
  }

  private def skip(char:Char): Unit = {
    skipWhitespaces()
    if (s.charAt(pos) == char) pos += 1
    else throw new ParseException(s"char $char not found in '$s'", pos)
  }

  private def readInt(): Int = {
    var intLength = 0
    while (s.charAt(pos).isDigit) {
      pos += 1
      intLength += 1
    }
    s.substring(pos-intLength, pos).toInt
  }

  def decode: (Int, String) = {
    skip('[')
    skipWhitespaces()
    val channelId:Int = readInt()
    skip(',')
    if (s.last != ']') throw new ParseException(s"data array does not end with ']': $s", s.length-1)
    skipWhitespaces()
    val payload = s.substring(pos, s.length-1)
    (channelId, payload)
  }
}