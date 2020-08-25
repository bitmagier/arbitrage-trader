package org.purevalue.arbitrage.trader

import java.time.{Duration, Instant, LocalDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.FooTrader.Trigger
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
  var pendingOrderBundles: Map[UUID, OrderBundle] = Map()
  var activeOrderBundles: Map[UUID, OrderBundle] = Map()
  var numSearchesTotal: Int = 0
  var numSearchesDiff: Int = 0
  var numSingleSearchesDiff: Int = 0
  var shotsDelivered: Int = 0
  var lastLifeSign: Instant = Instant.now()

  val scheduleDelay: FiniteDuration = FiniteDuration(config.getDuration("schedule-delay").toNanos, TimeUnit.NANOSECONDS)
  val schedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(30.seconds, scheduleDelay, self, Trigger())

  def newUUID(): UUID = UUID.randomUUID() // switch to Time based UUID when connecting a DB like cassandra

  trait NoResultReason
  case class BuyOrSellBookEmpty() extends NoResultReason
  case class BidAskGap() extends NoResultReason
  case class Confused() extends NoResultReason
  case class NoUSDTConversion(asses: Asset) extends NoResultReason
  case class MinGainTooLow() extends NoResultReason

  def findBestShotBasedOnTicker(tradePair: TradePair): (Option[OrderBundle], Option[NoResultReason]) = {
    // ignore wallet for now
    val ticker4Buy: Iterable[Ticker] = tc.tickers.map(_._2(tradePair))
    val ticker4Sell: Iterable[Ticker] = tc.tickers.map(_._2(tradePair))

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

    val orderBundleId = newUUID()
    val orderLimitAdditionRate: Double = config.getDouble("order-bundle.order-limit-addition-rate")
    val amountBaseAsset: Double = CryptoValue(Asset("USDT"), tradeQuantityUSDT).convertTo(tradePair.baseAsset, tc) match {
      case Some(v) => v.amount
      case None =>
        log.warn(s"Unable to convert ${tradePair.baseAsset} to USDT")
        return (None, Some(NoUSDTConversion(tradePair.baseAsset))) // only want to have assets convertible to USDT here
    }

    val ourBuyBaseAssetOrder =
      Order(
        newUUID(),
        orderBundleId,
        lowestAsk._1,
        tradePair,
        TradeSide.Buy,
        tc.fees(lowestAsk._1),
        amountBaseAsset,
        lowestAsk._2.price * (1.0d + orderLimitAdditionRate))

    val ourSellBaseAssetOrder =
      Order(
        newUUID(),
        orderBundleId,
        highestBid._1,
        tradePair,
        TradeSide.Sell,
        tc.fees(highestBid._1),
        amountBaseAsset,
        highestBid._2.price * (1.0d - orderLimitAdditionRate)
      )

    val bill: OrderBill = OrderBill.calc(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), tc)
    if (bill.sumUSDT >= config.getDouble("order-bundle.min-gain-in-usdt")) {
      (Some(OrderBundle(
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


  def findBestShotBasedOnOrderBook(tradePair: TradePair): (Option[OrderBundle], Option[NoResultReason]) = {
    // ignore wallet for now
    val books4Buy: Iterable[OrderBook] = tc.orderBooks.map(_._2(tradePair))
    val books4Sell: Iterable[OrderBook] = tc.orderBooks.map(_._2(tradePair))

    // safety check
    if (!books4Buy.forall(_.tradePair == tradePair)
      || !books4Sell.forall(_.tradePair == tradePair)) {
      throw new RuntimeException("safety-check failed")
    }

    if (books4Sell.isEmpty || books4Buy.isEmpty)
      return (None, Some(BuyOrSellBookEmpty()))

    val tradeQuantityUSDT = config.getDouble("order-bundle.trade-amount-in-usdt")

    val highestBid: Tuple2[String, Bid] = // that's what we try to sell to
      books4Sell
        .map(e => (e.exchange, e.highestBid))
        .maxBy(_._2.price)
    val lowestAsk: Tuple2[String, Ask] = // that's what we try to buy
      books4Buy
        .map(e => (e.exchange, e.lowestAsk))
        .minBy(_._2.price)

    if (highestBid._2.price <= lowestAsk._2.price) {
      return (None, Some(BidAskGap()))
    }

    if (highestBid._1 == lowestAsk._1) {
      log.warn(s"[$tradePair] found highest bid $highestBid and lowest ask $lowestAsk on the same exchange.")
      return (None, Some(Confused()))
    }

    val orderBundleId = newUUID()
    val orderLimitAdditionRate: Double = config.getDouble("order-bundle.order-limit-addition-rate")
    val amountBaseAsset: Double = CryptoValue(Asset("USDT"), tradeQuantityUSDT).convertTo(tradePair.baseAsset, tc) match {
      case Some(v) => v.amount
      case None =>
        log.warn(s"Unable to convert ${tradePair.baseAsset} to USDT")
        return (None, Some(NoUSDTConversion(tradePair.baseAsset))) // only want to have assets convertible to USDT here
    }

    val ourBuyBaseAssetOrder = Order(
      newUUID(),
      orderBundleId,
      lowestAsk._1,
      tradePair,
      TradeSide.Buy,
      tc.fees(lowestAsk._1),
      amountBaseAsset,
      lowestAsk._2.price * (1.0d + orderLimitAdditionRate))

    val ourSellBaseAssetOrder = Order(
      newUUID(),
      orderBundleId,
      highestBid._1,
      tradePair,
      TradeSide.Sell,
      tc.fees(highestBid._1),
      amountBaseAsset,
      highestBid._2.price * (1.0d - orderLimitAdditionRate)
    )

    val bill: OrderBill = OrderBill.calc(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), tc)
    if (bill.sumUSDT >= config.getDouble("order-bundle.min-gain-in-usdt")) {
      (Some(OrderBundle(
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

  def findBestShots(topN: Int): Seq[OrderBundle] = {
    var result: List[OrderBundle] = List()
    for (tradePair: TradePair <- tc.tickers.values.flatMap(_.keys).toSet) {
      if (tc.tickers.count(_._2.keySet.contains(tradePair)) > 1) {
        numSingleSearchesDiff += 1
        findBestShotBasedOnTicker(tradePair) match {

          case (Some(shot), _) if result.size < topN =>
            result = shot :: result

          case (Some(shot), _) if shot.bill.sumUSDT > result.map(_.bill.sumUSDT).min =>
            result = shot :: result.sortBy(_.bill.sumUSDT).tail

          case (Some(_), _) => // ignoring result

          case (None, Some(noResultReason)) =>
            noResultReasonStats += (noResultReason -> (1 + noResultReasonStats.getOrElse(noResultReason, 0)))

          case _ => throw new IllegalStateException()
        }
      }
    }
    result.sortBy(_.bill.sumUSDT).reverse
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

      val orderBookChoicesAggregated: Map[Int, Int] = tc.orderBooks
        .values
        .flatMap(_.keys)
        .foldLeft(Map[TradePair, Int]())((a, b) => a + (b -> (a.getOrElse(b, 0) + 1)))
        .values
        .foldLeft(Map[Int, Int]())((a, b) => a + (b -> (a.getOrElse(b, 0) + 1)))

      log.info(s"FooTrader TradeContext: TickerChoicesAggregated: $tickerChoicesAggregated, OrderBookChoicesAggregated: $orderBookChoicesAggregated")
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
        log.debug(s"Using TradeContext: with Tickers for Tradepairs[${tc.tickers.keys}]")
        lifeSign()
        numSearchesDiff += 1
        numSearchesTotal += 1
        findBestShots(maxOpenOrderBundles - pendingOrderBundles.size).foreach { b =>
          shotsDelivered += 1
          // TODO pendingOrderBundles += (orderBundle.id -> orderBundle)
          tradeRoom ! b
        }
      }

//    case OrderBundlePlaced(orderBundleId) =>
//      val ob = pendingOrderBundles(orderBundleId)
//      pendingOrderBundles = pendingOrderBundles - orderBundleId
//      activeOrderBundles += (orderBundleId -> ob)
//      log.info(s"FooTrader: $ob")
//
//    case OrderBundleCompleted(ob) =>
//      activeOrderBundles -= ob.orderBundle.id
//      log.info(s"FooTrader: $ob")
//
    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
