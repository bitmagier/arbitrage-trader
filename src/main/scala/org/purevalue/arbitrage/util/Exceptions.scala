package org.purevalue.arbitrage.util

case class WrongPrecondition(message:String) extends RuntimeException(message)
case class BadCalculationError(message:String) extends RuntimeException(message)
