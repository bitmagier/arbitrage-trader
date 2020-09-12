package org.purevalue.arbitrage.trader

import java.time.{Duration, Instant, LocalDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.purevalue.arbitrage.traderoom.Asset.USDT
import org.purevalue.arbitrage.traderoom.TradeRoom.TradeContext
import org.purevalue.arbitrage.traderoom._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object FooTrader {
  case class Trigger()

  def props(config: Config, tradeRoom: ActorRef, tc: TradeContext): Props = Props(new FooTrader(config, tradeRoom, tc))
}

/**
 * A basic trader to evolve the concept
 */
class FooTrader(config: Config, tradeRoom: ActorRef, tc: TradeContext) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[FooTrader])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val name: String = "FooTrader"
  val maxOpenOrderBundles: Int = config.getInt("max-open-order-bundles")
  var pendingOrderBundles: Map[UUID, OrderRequestBundle] = Map()
  var activeOrderBundles: Map[UUID, OrderRequestBundle] = Map()
  var numSearchesTotal: Int = 0
  var numSearchesDiff: Int = 0
  var numSingleSearchesDiff: Int = 0
  var shotsDelivered: Int = 0
  var lastLifeSign: Instant = Instant.now()

  val scheduleDelay: FiniteDuration = FiniteDuration(config.getDuration("schedule-delay").toNanos, TimeUnit.NANOSECONDS)
  val schedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(3.minutes, scheduleDelay, self, Trigger())

  sealed trait NoResultReason
  case class BuyOrSellBookEmpty() extends NoResultReason
  case class BidAskGap() extends NoResultReason
  case class Confused() extends NoResultReason
  case class NoUSDTConversion(asses: Asset) extends NoResultReason
  case class MinGainTooLow() extends NoResultReason

  def canTrade(t:Ticker): Boolean = !tc.doNotTouch(t.exchange).contains(t.tradePair.involvedAssets)

  def findBestShotBasedOnTicker(tradePair: TradePair): (Option[OrderRequestBundle], Option[NoResultReason]) = {
    // ignore wallet for now
    val ticker4Buy: Iterable[Ticker] = tc.tickers.map(_._2(tradePair)).filter(canTrade)
    val ticker4Sell: Iterable[Ticker] = tc.tickers.map(_._2(tradePair)).filter(canTrade)

    // safety check
    if (!ticker4Buy.forall(_.tradePair == tradePair)
      || !ticker4Sell.forall(_.tradePair == tradePair)) {
      throw new RuntimeException("safety-check failed")
    }

    if (ticker4Sell.isEmpty || ticker4Buy.isEmpty)
      return (None, Some(BuyOrSellBookEmpty()))

    val tradeQuantityUSDT = config.getDouble("order-bundle.trade-amount-in-usdt")

    val highestBid: Tuple2[String, Bid] = // that's what we try to sell to
      ticker4Sell
        .map(e => (e.exchange, Bid(e.highestBidPrice, e.highestBidQuantity.getOrElse(1))))
        .maxBy(_._2.price)
    val lowestAsk: Tuple2[String, Ask] = // that's what we try to buy
      ticker4Buy
        .map(e => (e.exchange, Ask(e.lowestAskPrice, e.lowestAskQuantity.getOrElse(1))))
        .minBy(_._2.price)

    if (highestBid._2.price <= lowestAsk._2.price) {
      return (None, Some(BidAskGap()))
    }

    if (highestBid._1 == lowestAsk._1) {
      log.warn(s"[$tradePair] found highest bid $highestBid and lowest ask $lowestAsk on the same exchange.")
      return (None, Some(Confused()))
    }

    val orderBundleId = UUID.randomUUID()
    val orderLimitAdditionRate: Double = config.getDouble("order-bundle.order-limit-addition-rate")

    val buyExchange: String = lowestAsk._1
    val sellExchange: String = highestBid._1

    // only want to have assets convertible to USDT here, to ignore the (~3%) complicated special cases for now
    if (!CryptoValue(USDT, tradeQuantityUSDT).canConvertTo(tradePair.baseAsset, tc.tickers(buyExchange)) ||
      !CryptoValue(USDT, tradeQuantityUSDT).canConvertTo(tradePair.baseAsset, tc.tickers(sellExchange))) {
      log.warn(s"Unable to convert ${tradePair.baseAsset} to USDT")
      return (None, Some(NoUSDTConversion(tradePair.baseAsset)))
    }

    val amountBaseAsset: Double = CryptoValue(USDT, tradeQuantityUSDT).convertTo(tradePair.baseAsset, tc.referenceTicker).amount

    val ourBuyBaseAssetOrder =
      OrderRequest(
        UUID.randomUUID(),
        Some(orderBundleId),
        buyExchange,
        tradePair,
        TradeSide.Buy,
        tc.fees(buyExchange),
        amountBaseAsset / (1.0 - tc.fees(buyExchange).average), // usually we have to buy X + fee, because fee gets substracted; an exeption is on binance when paying with BNB
        lowestAsk._2.price * (1.0d + orderLimitAdditionRate))

    val ourSellBaseAssetOrder =
      OrderRequest(
        UUID.randomUUID(),
        Some(orderBundleId),
        sellExchange,
        tradePair,
        TradeSide.Sell,
        tc.fees(sellExchange),
        amountBaseAsset,
        highestBid._2.price * (1.0d - orderLimitAdditionRate)
      )

    val bill: OrderBill = OrderBill.calc(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), tc.tickers)
    if (bill.sumUSDTAtCalcTime >= config.getDouble("order-bundle.min-gain-in-usdt")) {
      (Some(OrderRequestBundle(
        orderBundleId,
        name,
        self,
        LocalDateTime.now(),
        List(ourBuyBaseAssetOrder, ourSellBaseAssetOrder),
        bill
      )), None)
    } else {
      (None, Some(MinGainTooLow()))
    }
  }

  var noResultReasonStats: Map[NoResultReason, Int] = Map()

  def findBestShots(topN: Int): Seq[OrderRequestBundle] = {
    var result: List[OrderRequestBundle] = List()
    for (tradePair: TradePair <- tc.tickers.values.flatMap(_.keys).toSet) {
      if (tc.tickers.count(_._2.keySet.contains(tradePair)) > 1) {
        numSingleSearchesDiff += 1
        findBestShotBasedOnTicker(tradePair) match {

          case (Some(shot), _) if result.size < topN =>
            result = shot :: result

          case (Some(shot), _) if shot.bill.sumUSDTAtCalcTime > result.map(_.bill.sumUSDTAtCalcTime).min =>
            result = shot :: result.sortBy(_.bill.sumUSDTAtCalcTime).tail

          case (Some(_), _) => // ignoring result

          case (None, Some(noResultReason)) =>
            noResultReasonStats += (noResultReason -> (1 + noResultReasonStats.getOrElse(noResultReason, 0)))

          case _ => throw new IllegalStateException()
        }
      }
    }
    result.sortBy(_.bill.sumUSDTAtCalcTime).reverse
  }

  def lifeSign(): Unit = {
    val duration = Duration.between(lastLifeSign, Instant.now())
    if (duration.compareTo(config.getDuration("lifesign-interval")) > 0) {
      log.info(s"FooTrader life sign: $shotsDelivered shots delivered. $numSearchesDiff search runs ($numSingleSearchesDiff single searches) done in last ${duration.toMinutes} minutes. Total search runs: $numSearchesTotal")
      val tickerChoicesAggregated: Map[Int, Int] = tc.tickers
        .values
        .flatMap(_.keys)
        .foldLeft(Map[TradePair, Int]())((a, b) => a + (b -> (a.getOrElse(b, 0) + 1)))
        .values
        .foldLeft(Map[Int, Int]())((a, b) => a + (b -> (a.getOrElse(b, 0) + 1)))

      log.info(s"FooTrader no-result-reasons: $noResultReasonStats")
      lastLifeSign = Instant.now()
      numSingleSearchesDiff = 0
      numSearchesDiff = 0
    }
  }

  log.info("FooTrader running")

  override def receive: Receive = {

    // from Scheduler

    case Trigger() =>
      if (pendingOrderBundles.size < maxOpenOrderBundles) {
        log.trace(s"Using TradeContext: with Tickers for Tradepairs[${tc.tickers.keys.mkString(",")}]")
        lifeSign()
        numSearchesDiff += 1
        numSearchesTotal += 1
        findBestShots(maxOpenOrderBundles - pendingOrderBundles.size).foreach { b =>
          shotsDelivered += 1
          tradeRoom ! b
        }
      }

    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
