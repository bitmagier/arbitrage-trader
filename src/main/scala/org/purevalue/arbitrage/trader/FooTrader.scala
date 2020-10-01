package org.purevalue.arbitrage.trader

import java.time.{Duration, Instant, LocalDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.purevalue.arbitrage.traderoom.TradeRoom.TradeContext
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.OrderLimitChooser
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object FooTrader {
  case class Trigger()

  def props(traderConfig: Config, tradeRoom: ActorRef, tc: TradeContext): Props = Props(new FooTrader(traderConfig, tradeRoom, tc))
}

/**
 * A basic trader to evolve the concept
 */
class FooTrader(traderConfig: Config, tradeRoom: ActorRef, tc: TradeContext) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[FooTrader])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val traderName: String = "FooTrader"
  val maxOpenOrderBundles: Int = traderConfig.getInt("max-open-order-bundles")
  var pendingOrderBundles: Map[UUID, OrderRequestBundle] = Map()
  var activeOrderBundles: Map[UUID, OrderRequestBundle] = Map()
  var numSearchesTotal: Int = 0
  var numSearchesDiff: Int = 0
  var numSingleSearchesDiff: Int = 0
  var shotsDelivered: Int = 0
  var lastLifeSign: Instant = Instant.now()

  val OrderLimitRealityAdjustmentRate: Double = traderConfig.getDouble("order-bundle.order-limit-addition-rate")
  val OrderBundleMinGainInUSD: Double = traderConfig.getDouble("order-bundle.min-gain-in-usd")
  val TradeAmountInUSD: Double = traderConfig.getDouble("order-bundle.trade-amount-in-usd")

  val scheduleDelay: FiniteDuration = FiniteDuration(traderConfig.getDuration("schedule-delay").toNanos, TimeUnit.NANOSECONDS)
  val schedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(3.minutes, scheduleDelay, self, Trigger())

  sealed trait NoResultReason
  case class NotEnoughExchangesAvailableForTrading() extends NoResultReason
  case class BuyOrSellBookEmpty() extends NoResultReason
  case class BidAskGap() extends NoResultReason
  case class Confused() extends NoResultReason
  case class NoUSDTConversion(asses: Asset) extends NoResultReason
  case class MinGainTooLow() extends NoResultReason
  case class MinGainTooLow2() extends NoResultReason

  def canTrade(exchange: String, tradePair: TradePair): Boolean =
    !tc.doNotTouch(exchange).contains(tradePair.baseAsset) && !tc.doNotTouch(exchange).contains(tradePair.quoteAsset)

  // finds (average) exchange rate based on reference ticker, if tradepair is available there
  // otherwise ticker rate is retrieved from fallBackTickerExchanges
  def findPrice(tradePair: TradePair, fallBackTickerExchanges: Iterable[String]): Option[Double] = {
    def _findPrice(exchangeOptions: List[String]): Option[Double] = {
      if (exchangeOptions.isEmpty) None
      else tc.tickers(exchangeOptions.head).get(tradePair)
        .map(_.priceEstimate)
        .orElse(_findPrice(exchangeOptions.tail))
    }

    val exchangesInOrder = tc.referenceTickerExchange :: fallBackTickerExchanges.filterNot(_ == tc.referenceTickerExchange).toList
    _findPrice(exchangesInOrder)
  }

  def determineLimit(exchange: String, tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double): Option[Double] = {
    new OrderLimitChooser(tc.orderBooks(exchange).get(tradePair), tc.tickers(exchange)(tradePair))
      .determineRealisticOrderLimit(tradeSide, amountBaseAsset, OrderLimitRealityAdjustmentRate)
  }

  def findBestShot(tradePair: TradePair): Either[OrderRequestBundle, NoResultReason] = {
    val availableExchanges: Iterable[String] =
      tc.tradePairs
        .filter(_._2.contains(tradePair))
        .filter(e => canTrade(e._1, tradePair))
        .keys

    if (availableExchanges.size <= 1) return Right(NotEnoughExchangesAvailableForTrading())

    val USDT = Asset("USDT")
    val tradeAmountBaseAsset: Double = CryptoValue(USDT, TradeAmountInUSD)
      .convertTo(tradePair.baseAsset, tp => findPrice(tp, availableExchanges))
      .amount

    val buyLimits: Map[String, Double] = availableExchanges
      .map(exchange => exchange -> determineLimit(exchange, tradePair, TradeSide.Buy, tradeAmountBaseAsset))
      .filter(_._2.isDefined)
      .map(e => e._1 -> e._2.get)
      .toMap

    val sellLimits: Map[String, Double] = availableExchanges
      .map(exchange => exchange -> determineLimit(exchange, tradePair, TradeSide.Sell, tradeAmountBaseAsset))
      .filter(_._2.isDefined)
      .map(e => e._1 -> e._2.get)
      .toMap

    val buyExchange: String = buyLimits.minBy(_._2)._1
    val sellExchange: String = sellLimits.maxBy(_._2)._1
    val buyLimit: Double = buyLimits(buyExchange)
    val sellLimit: Double = sellLimits(sellExchange)

    val minGainInUSD: Double = OrderBundleMinGainInUSD

    val calculatedPureWinUSD: Double = CryptoValue(tradePair.baseAsset, tradeAmountBaseAsset * sellLimit - tradeAmountBaseAsset * buyLimit)
      .convertTo(USDT, tp => findPrice(tp, availableExchanges)).amount

    if (calculatedPureWinUSD < minGainInUSD) return Right(MinGainTooLow())

    val orderBundleId = UUID.randomUUID()
    val ourBuyBaseAssetOrder =
      OrderRequest(
        UUID.randomUUID(),
        Some(orderBundleId),
        buyExchange,
        tradePair,
        TradeSide.Buy,
        tc.feeRates(buyExchange),
        tradeAmountBaseAsset / (1.0 - tc.feeRates(buyExchange)), // usually we have to buy X + fee, because fee gets substracted; an exeption is on binance when paying with BNB
        buyLimit
      )
    val ourSellBaseAssetOrder =
      OrderRequest(
        UUID.randomUUID(),
        Some(orderBundleId),
        sellExchange,
        tradePair,
        TradeSide.Sell,
        tc.feeRates(sellExchange),
        tradeAmountBaseAsset,
        sellLimit
      )

    // TODO don't use ticker, use reference-ticker
    val bill: OrderBill = OrderBill.calc(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), Asset.AssetUSDT, tc.referenceTicker)
    if (bill.sumUSDAtCalcTime < OrderBundleMinGainInUSD) {
      Right(MinGainTooLow2())
    } else {
      Left(OrderRequestBundle(
        orderBundleId,
        traderName,
        Instant.now(),
        List(ourBuyBaseAssetOrder, ourSellBaseAssetOrder),
        bill
      ))
    }
  }

  //  def findBestShotBasedOnTicker(tradePair: TradePair): Either[OrderRequestBundle, NoResultReason] = {
  //    // ignore wallet for now
  //    val ticker4Buy: Iterable[Ticker] = tc.tickers.map(_._2(tradePair)).filter(e => canTrade(e.exchange, e.tradePair))
  //    val ticker4Sell: Iterable[Ticker] = tc.tickers.map(_._2(tradePair)).filter(e => canTrade(e.exchange, e.tradePair))
  //
  //    // safety check
  //    if (!ticker4Buy.forall(_.tradePair == tradePair)
  //      || !ticker4Sell.forall(_.tradePair == tradePair)) {
  //      throw new RuntimeException("safety-check failed")
  //    }
  //
  //    if (ticker4Sell.isEmpty || ticker4Buy.isEmpty)
  //      return Right(BuyOrSellBookEmpty())
  //
  //    val highestBid: Tuple2[String, Bid] = // that's what we try to sell to
  //      ticker4Sell
  //        .map(e => (e.exchange, Bid(e.highestBidPrice, e.highestBidQuantity.getOrElse(1))))
  //        .maxBy(_._2.price)
  //    val lowestAsk: Tuple2[String, Ask] = // that's what we try to buy
  //      ticker4Buy
  //        .map(e => (e.exchange, Ask(e.lowestAskPrice, e.lowestAskQuantity.getOrElse(1))))
  //        .minBy(_._2.price)
  //
  //    if (highestBid._2.price <= lowestAsk._2.price) {
  //      return Right(BidAskGap())
  //    }
  //
  //    if (highestBid._1 == lowestAsk._1) {
  //      log.warn(s"[$tradePair] found highest bid $highestBid and lowest ask $lowestAsk on the same exchange.")
  //      return Right(Confused())
  //    }
  //
  //    val orderBundleId = UUID.randomUUID()
  //
  //    val buyExchange: String = lowestAsk._1
  //    val sellExchange: String = highestBid._1
  //
  //    // only want to have assets convertible to USDT here, to ignore the (~3%) complicated special cases for now
  //    if (!CryptoValue(USDT, TradeAmountInUsdt).canConvertTo(tradePair.baseAsset, tc.tickers(buyExchange)) ||
  //      !CryptoValue(USDT, TradeAmountInUsdt).canConvertTo(tradePair.baseAsset, tc.tickers(sellExchange))) {
  //      log.warn(s"Unable to convert ${tradePair.baseAsset} to USDT")
  //      return Right(NoUSDTConversion(tradePair.baseAsset))
  //    }
  //
  //    val amountBaseAsset: Double = CryptoValue(USDT, TradeAmountInUsdt).convertTo(tradePair.baseAsset, tc.referenceTicker).amount
  //    val buyLimit = new OrderLimitChooser(tc.orderBooks.get(buyExchange).map(_(tradePair)), tc.tickers(buyExchange)(tradePair))
  //      .determineRealisticOrderLimit(TradeSide.Buy, amountBaseAsset, OrderLimitRealityAdjustmentRate)
  //
  //    val ourBuyBaseAssetOrder =
  //      OrderRequest(
  //        UUID.randomUUID(),
  //        Some(orderBundleId),
  //        buyExchange,
  //        tradePair,
  //        TradeSide.Buy,
  //        tc.fees(buyExchange),
  //        amountBaseAsset / (1.0 - tc.fees(buyExchange).average), // usually we have to buy X + fee, because fee gets substracted; an exeption is on binance when paying with BNB
  //        buyLimit
  //      )
  //
  //    val sellLimit = new OrderLimitChooser(tc.orderBooks.get(sellExchange).map(_(tradePair)), tc.tickers(sellExchange)(tradePair))
  //      .determineRealisticOrderLimit(TradeSide.Sell, amountBaseAsset, OrderLimitRealityAdjustmentRate)
  //
  //    val ourSellBaseAssetOrder =
  //      OrderRequest(
  //        UUID.randomUUID(),
  //        Some(orderBundleId),
  //        sellExchange,
  //        tradePair,
  //        TradeSide.Sell,
  //        tc.fees(sellExchange),
  //        amountBaseAsset,
  //        sellLimit
  //      )
  //
  //    val bill: OrderBill = OrderBill.calc(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), tc.tickers)
  //    if (bill.sumUSDTAtCalcTime >= OrderBundleMinGainInUsdt) {
  //      Left(OrderRequestBundle(
  //        orderBundleId,
  //        traderName,
  //        LocalDateTime.now(),
  //        List(ourBuyBaseAssetOrder, ourSellBaseAssetOrder),
  //        bill
  //      ))
  //    } else {
  //      Right(MinGainTooLow())
  //    }
  //  }

  var noResultReasonStats: Map[NoResultReason, Int] = Map()

  def findBestShots(topN: Int): Seq[OrderRequestBundle] = {
    var result: List[OrderRequestBundle] = List()
    for (tradePair: TradePair <- tc.tickers.values.flatMap(_.keys).toSet) {
      if (tc.tickers.count(_._2.keySet.contains(tradePair)) > 1) {
        numSingleSearchesDiff += 1
        findBestShot(tradePair) match {

          case Left(shot) if result.size < topN =>
            result = shot :: result

          case Left(shot) if shot.bill.sumUSDAtCalcTime > result.map(_.bill.sumUSDAtCalcTime).min =>
            result = shot :: result.sortBy(_.bill.sumUSDAtCalcTime).tail

          case Left(_) => // ignoring result

          case Right(noResultReason) =>
            noResultReasonStats += (noResultReason -> (1 + noResultReasonStats.getOrElse(noResultReason, 0)))

          case _ => throw new IllegalStateException()
        }
      }
    }
    result.sortBy(_.bill.sumUSDAtCalcTime).reverse
  }

  def lifeSign(): Unit = {
    val duration = Duration.between(lastLifeSign, Instant.now())
    if (duration.compareTo(traderConfig.getDuration("lifesign-interval")) > 0) {
      log.info(s"FooTrader life sign: $shotsDelivered shots delivered. $numSearchesDiff search runs ($numSingleSearchesDiff single searches) done in last ${duration.toMinutes} minutes. Total search runs: $numSearchesTotal")

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
