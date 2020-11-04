package org.purevalue.arbitrage.trader

import java.time.{Duration, Instant}

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.purevalue.arbitrage.ExchangeConfig
import org.purevalue.arbitrage.trader.TemporaryLowDetector.{LogStats, SearchRun}
import org.purevalue.arbitrage.traderoom.{TradeContext, TradePair}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

object TemporaryLowDetector {
  def apply(exchangesConfig: Map[String, ExchangeConfig]):
  Behavior[Command] = {
    Behaviors.withTimers(timers =>
      Behaviors.setup(context => new TemporaryLowDetector(context, timers, exchangesConfig)))
  }

  sealed trait Command
  case class SearchRun(tc: TradeContext) extends Command
  case class LogStats() extends Command
}
class TemporaryLowDetector(context: ActorContext[TemporaryLowDetector.Command],
                           timers: TimerScheduler[TemporaryLowDetector.Command],
                           exchangesConfig: Map[String, ExchangeConfig])
  extends AbstractBehavior[TemporaryLowDetector.Command](context) {

  private val log = LoggerFactory.getLogger(getClass)
  private val MinLowDuration = Duration.ofSeconds(2)
  private val MaxLowDuration = Duration.ofMinutes(20)
  private val LowEventBeginThreshold: Double = 0.995
  private val LowEventEndThreshold: Double = 0.999

  case class LowEventKey(exchange: String, pair: TradePair, startingTime: Instant)
  case class MeasuringPoint(time: Instant, price: Double, mainStreamPrice: Double)
  case class LowEvent(key: LowEventKey, var points: Vector[MeasuringPoint], var ended: Boolean) {

    def snapshots(num:Int): Seq[Double] = {
      val inc: Double = (points.size - 1) / num
      val indexes: Seq[Int] = (0 to num).map(_ * inc).map(_.round.toInt)
      indexes.map(i => points(i)).map(e => e.price / e.mainStreamPrice)
    }

    override def toString: String = s"${key.exchange} ${key.pair} ${key.startingTime} " +
      s"""${Duration.between(key.startingTime, points.last.time).toSeconds} s: ${snapshots(6).map(e => formatDecimal(e, 4, 4)).mkString(", ")}}"""
  }

  var events: Map[LowEventKey, LowEvent] = Map()

  def newEvent(event: LowEvent): Unit = {
    events = events + (event.key -> event)
  }

  def openEvents: Iterable[LowEvent] = {
    events.values.filter(e =>
      !e.ended &&
        e.key.startingTime.isAfter(Instant.now.minus(MaxLowDuration)))
  }

  def eventSeemsEnded(event: LowEvent): Boolean = {
    val now = Instant.now
    val duration = Duration.between(event.key.startingTime, now)
    if (!event.ended && duration.compareTo(MinLowDuration) < 0) return false
    if (event.ended || duration.compareTo(MaxLowDuration) > 0) return true

    val priceLevel: Double = event.points.last.price / event.points.last.mainStreamPrice
    priceLevel >= LowEventEndThreshold
  }

  def inNewEventZone(price: Double, mainStreamPrice: Double): Boolean = {
    val level = price / mainStreamPrice
    level <= LowEventBeginThreshold
  }

  def addMeasuringPoint(event: LowEvent, point: MeasuringPoint): Unit = {
    events(event.key).points = events(event.key).points :+ point
    if (eventSeemsEnded(event)) {
      event.ended = true
    }
  }

  def average(values: Iterable[Double]): Double = values.sum / values.size

  def preparePrices(tc: TradeContext): Map[TradePair, Map[String, Double]] = {
    // pairs with more than one exchange
    val pairs: Iterable[TradePair] = tc.tradePairs.values.flatten
      .foldLeft(Map[TradePair, Int]())((a, b) => if (a.contains(b)) a + (b -> (a(b) + 1)) else a + (b -> 1))
      .filter(_._2 > 1)
      .keys

    var result: Map[TradePair, Map[String, Double]] = Map()
    for (pair <- pairs) {
      var subMap: Map[String, Double] = Map()
      val exchanges = tc.tradePairs.filter(_._2.contains(pair)).keys
      for (exchange <- exchanges) {
        val price = if (exchangesConfig(exchange).tickerIsRealtime)
          tc.tickers(exchange)(pair).priceEstimate
        else tc.orderBooks(exchange)(pair).lowestAsk.price
        subMap = subMap + (exchange -> price)
      }
      result = result + (pair -> subMap)
    }
    result
  }

  def searchRun(tc: TradeContext): Unit = {
    val time = Instant.now
    val open: Iterable[LowEvent] = openEvents

    val prices: Map[TradePair, Map[String, Double]] = preparePrices(tc)
    for (pair <- prices.keySet) {
      open.find(e => e.key.pair == pair) match {

        case Some(lowEvent) =>
          val point = MeasuringPoint(
            time,
            prices(pair)(lowEvent.key.exchange),
            average(
              prices(pair).filterNot(_._1 == lowEvent.key.exchange).values
            )
          )
          addMeasuringPoint(lowEvent, point)

        case None =>
          val lowest: (String, Double) = prices(pair).minBy(_._2)
          val mainstreamPrice = average(
            prices(pair).filterNot(_._1 == lowest._1).values
          )
          if (inNewEventZone(lowest._2, mainstreamPrice)) {
            newEvent(
              LowEvent(
                LowEventKey(lowest._1, pair, time),
                Vector(MeasuringPoint(time, lowest._2, mainstreamPrice)),
                ended = false
              )
            )
          }
      }
    }
  }

  def logStats(): Unit = {
    for (event <- events.values.filter(_.ended)) {
      log.info(s"event: $event")
    }
  }

  override def onMessage(message: TemporaryLowDetector.Command): Behavior[TemporaryLowDetector.Command] = {
    message match {
      case SearchRun(tc) => searchRun(tc)
      case LogStats() => logStats()
    }
    this
  }

  timers.startTimerAtFixedRate(LogStats(), 30.minutes)
}
