package org.purevalue.arbitrage.trader

import java.time.{Duration, Instant}
import java.util.UUID

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.OrderLimitChooser

import scala.concurrent.ExecutionContextExecutor

object FooTrader {
  def props(traderConfig: Config, tradeRoom: ActorRef): Props = Props(new FooTrader(traderConfig, tradeRoom))

  sealed trait Command
  case class SearchRun(tc: TradeContext) extends Command
}

/**
 * A basic trader to evolve the concept
 */
class FooTrader(traderConfig: Config, tradeRoom: ActorRef) extends Actor with ActorLogging {
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

  val OrderbookBasedLimitQuantityOverbookingRate: Double = traderConfig.getDouble("order-bundle.orderbook-based-tx-limit-quantity-overbooking")
  val TickerBasedOrderLimitRealityAdjustmentRate: Double = traderConfig.getDouble("order-bundle.ticker-based-tx-limit-beyond-edge-limit")
  val OrderBundleMinGainInUSD: Double = traderConfig.getDouble("order-bundle.min-gain-in-usd")
  val TradeAmountInUSD: Double = traderConfig.getDouble("order-bundle.trade-amount-in-usd")

  sealed trait NoResultReason
  case class NotEnoughExchangesAvailableForTrading() extends NoResultReason
  case class BuyOrSellBookEmpty() extends NoResultReason
  case class BidAskGap() extends NoResultReason
  case class Confused() extends NoResultReason
  case class NoUSDTConversion(asses: Asset) extends NoResultReason
  case class MinGainTooLow() extends NoResultReason
  case class MinGainTooLow2() extends NoResultReason

  def canTrade(exchange: String, tradePair: TradePair)(implicit tc: TradeContext): Boolean =
    !tc.doNotTouch(exchange).contains(tradePair.baseAsset) && !tc.doNotTouch(exchange).contains(tradePair.quoteAsset)

  // finds (average) exchange rate based on reference ticker, if tradepair is available there
  // otherwise ticker rate is retrieved from fallBackTickerExchanges
  def findPrice(tradePair: TradePair, fallBackTickerExchanges: Iterable[String])(implicit tc: TradeContext): Option[Double] = {
    def _findPrice(exchangeOptions: List[String]): Option[Double] = {
      if (exchangeOptions.isEmpty) None
      else tc.tickers(exchangeOptions.head).get(tradePair)
        .map(_.priceEstimate)
        .orElse(_findPrice(exchangeOptions.tail))
    }

    val exchangesInOrder = tc.referenceTickerExchange :: fallBackTickerExchanges.filterNot(_ == tc.referenceTickerExchange).toList
    _findPrice(exchangesInOrder)
  }

  def determineLimit(exchange: String, tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double)(implicit tc: TradeContext): Option[Double] = {
    new OrderLimitChooser(tc.orderBooks(exchange).get(tradePair), tc.tickers(exchange)(tradePair))
      .determineRealisticOrderLimit(tradeSide, amountBaseAsset, OrderbookBasedLimitQuantityOverbookingRate, TickerBasedOrderLimitRealityAdjustmentRate)
  }

  def findBestShot(tradePair: TradePair)(implicit tc: TradeContext): Either[OrderRequestBundle, NoResultReason] = {
    val availableExchanges: Iterable[String] =
      tc.tradePairs
        .filter(_._2.contains(tradePair))
        .filter(e => canTrade(e._1, tradePair))
        .keys

    if (availableExchanges.size <= 1) return Right(NotEnoughExchangesAvailableForTrading())

    val usdEquivatentCalcCoin: Asset = Asset.UsdEquivalentCoins
      .find(_.canConvertTo(tradePair.baseAsset, tp => findPrice(tp, availableExchanges).isDefined)).get // there must be one exchange having that trade pair
    val tradeAmountBaseAsset: Double =
      CryptoValue(usdEquivatentCalcCoin, TradeAmountInUSD)
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
      .convertTo(usdEquivatentCalcCoin, tp => findPrice(tp, availableExchanges)).amount

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

    val bill: OrderBill = OrderBill.calc(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), usdEquivatentCalcCoin, tc.referenceTicker)
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

  var noResultReasonStats: Map[NoResultReason, Int] = Map()

  def findBestShots(topN: Int)(implicit tc: TradeContext): Seq[OrderRequestBundle] = {
    var result: List[OrderRequestBundle] = List()
    val cryptoTradePairs = tc.tickers.values.flatMap(_.keys).filterNot(_.involvedAssets.exists(_.isFiat))
    for (tradePair: TradePair <- cryptoTradePairs) {
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

  override def receive: Receive = {
    case SearchRun(tc) =>
      lifeSign()
      numSearchesDiff += 1
      numSearchesTotal += 1
      findBestShots(3)(tc).foreach { b =>
        shotsDelivered += 1
        tradeRoom ! b
      }

    case Failure(cause)  => log.error(cause, "Failure received")
  }

  log.info("FooTrader started")
}
