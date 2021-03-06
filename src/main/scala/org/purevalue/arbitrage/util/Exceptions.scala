package org.purevalue.arbitrage.util

final class WrongAssumption(message: String) extends RuntimeException(message)
final class BadCalculationError(message: String) extends RuntimeException(message)
final class IncomingDataError(message: String) extends RuntimeException(message)
final class ConnectionClosedException(message: String) extends RuntimeException(message)
final class StaleDataException(message: String) extends RuntimeException(message)
final class InitializationException(message: String) extends RuntimeException(message)