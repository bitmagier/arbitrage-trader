package org.purevalue.arbitrage.util

final class WrongAssumption(message: String) extends RuntimeException(message)
final class BadCalculationError(message: String) extends RuntimeException(message)
final class IncomingDataError(message: String) extends RuntimeException(message)
final class ExchangeDataOutdated(message:String) extends RuntimeException(message)