package org.purevalue.arbitrage.trader

import java.time.{Duration, Instant, LocalDateTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.TradepairDataManager.{Ask, Bid}
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object FooTrader {
  case class Trigger()

  def props(config: Config, tradeRoom: ActorRef): Props = Props(new FooTrader(config, tradeRoom))
}

/**
 * A basic trader to evolve the concept
 */
class FooTrader(config: Config, tradeRoom: ActorRef) extends Actor {
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
  var lastLifeSigh: Instant = Instant.now()

  val scheduleRate: FiniteDuration = FiniteDuration(config.getDuration("schedule-rate").toNanos, TimeUnit.NANOSECONDS)

  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(10.seconds, scheduleRate, self, Trigger())

  def newUUID(): UUID = UUID.randomUUID() // switch to Time based UUID when connecting a DB like cassandra

  def calculateWinUSDT(orders: Seq[Order], dc: TradeDecisionContext): Option[Double] = {
    val invoice: Seq[CryptoValue] = orders.flatMap(_.bill)
    val converted = invoice.map(_.convertTo(Asset("USDT"), dc))
    if (converted.forall(_.isDefined)) {
      Some(converted.map(_.get).sum)
    } else {
      None
    }
  }

  def convertToUSDT(amount: Double, asset: Asset, dc: TradeDecisionContext): Option[Double] =
    CryptoValue(asset, amount).convertTo(Asset("USDT"), dc)

  def findBestShotBasedOnOrderBook(tradePair: TradePair, dc: TradeDecisionContext): Option[OrderBundle] = {
//    val minBalanceBeforeTradeInUSDT = config.getDouble("order-bundle.min-balance-before-trade-in-usdt")
//    val whatsSpendablePerExchange: Map[String, Set[Asset]] =
//      dc.walletPerExchange
//        .map(pair => (
//          pair._1,
//          pair._2.assets
//            .filter(a => {
//              val usdt = convertToUSDT(a._2, a._1, dc);
//              usdt.isDefined && usdt.get >= minBalanceBeforeTradeInUSDT
//            })
//            .keySet))

//    val books4Buy = dc.orderBooks(tradePair).values
//      .filter(b => whatsSpendablePerExchange(b.exchange).contains(b.tradePair.quoteAsset))
//
//    val books4Sell = dc.orderBooks(tradePair).values
//      .filter(b => whatsSpendablePerExchange(b.exchange).contains(b.tradePair.baseAsset))

    // ignore wallet for now
    val books4Buy = dc.orderBooks(tradePair).values
    val books4Sell = dc.orderBooks(tradePair).values

    // safety check
    if (!books4Buy.forall(_.tradePair == tradePair)
      || !books4Sell.forall(_.tradePair == tradePair)) {
      throw new RuntimeException("safety-check failed")
    }

    if (books4Sell.isEmpty || books4Buy.isEmpty)
      return None

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
      return None
    }

    if (highestBid._1 == lowestAsk._1) {
      log.warn(s"${Emoji.SadAndConfused} [$tradePair] found highest bid $highestBid and lowest ask $lowestAsk on the same exchange.")
      return None
    }

    val orderBundleId = newUUID()
    val orderLimitAdditionPct: Double = config.getDouble("order-bundle.order-limit-addition-percentage")
    val amountBaseAsset = CryptoValue(Asset("USDT"), tradeQuantityUSDT).convertTo(tradePair.baseAsset, dc)
    if (amountBaseAsset.isEmpty)
      return None // only want to have assets convertible to USDT here

    val ourBuyBaseAssetOrder = Order(
      newUUID(),
      orderBundleId,
      lowestAsk._1,
      tradePair,
      TradeDirection.Buy,
      dc.feePerExchange(lowestAsk._1),
      amountBaseAsset,
      None,
      lowestAsk._2.price * (1.0d + orderLimitAdditionPct * 0.01d))

    val amountQuoteAsset = CryptoValue(Asset("USDT"), tradeQuantityUSDT).convertTo(tradePair.quoteAsset, dc)
    if (amountQuoteAsset.isEmpty)
      return None

    val ourSellBaseAssetOrder = Order(
      newUUID(),
      orderBundleId,
      highestBid._1,
      tradePair,
      TradeDirection.Sell,
      dc.feePerExchange(highestBid._1),
      None,
      amountQuoteAsset,
      highestBid._2.price * (1.0d - orderLimitAdditionPct * 0.01d)
    )

    val estimatedWinUSDT: Option[Double] = calculateWinUSDT(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), dc)
    if (estimatedWinUSDT.isDefined && estimatedWinUSDT.get >= config.getDouble("order-bundle.min-gain-in-usdt")) {
      Some(OrderBundle(
        orderBundleId,
        name,
        self,
        LocalDateTime.now(),
        List(ourBuyBaseAssetOrder, ourSellBaseAssetOrder),
        estimatedWinUSDT.get,
        ""
      ))
    } else {
      None
    }
  }

  def findBestShot(dc: TradeDecisionContext): Option[OrderBundle] = {
    var result: Option[OrderBundle] = None
    for (tradePair <- dc.orderBooks.keySet) {
      if (dc.orderBooks(tradePair).size > 1) {
        numSingleSearchesDiff += 1
        findBestShotBasedOnOrderBook(tradePair, dc) match {
          case Some(shot) => if (result.isEmpty || shot.estimatedWinUSDT > result.get.estimatedWinUSDT) {
            result = Some(shot)
          }
          case None =>
        }
      }
    }
    result
  }

  def lifeSign(dc: TradeDecisionContext): Unit = {
    val minutes = Duration.between(lastLifeSigh, Instant.now()).toMinutes
    if (minutes > 3) {
      val numTradepairPerTickerAmount : Map[Int, Int] = dc.tickers
        .map(e => e._2.size)
        .foldLeft(Map[Int, Int]())((a,b) => a + (b -> a.getOrElse(b, 0)))
      val tickerStats:String = numTradepairPerTickerAmount.foldLeft("")((a,b) => a + s"[${b._1} ticker: ${b._2}] ")
      val numTradepairPerOrderBookAmount : Map[Int, Int] = dc.orderBooks
        .map(e => e._2.size)
        .foldLeft(Map[Int, Int]())((a,b) => a + (b -> a.getOrElse(b, 0)))
      val orderBookStats:String = numTradepairPerOrderBookAmount.foldLeft("")((a,b) => a + s"[${b._1} orderbooks: ${b._2}] ")
      log.info(s"Life sign: $numSearchesDiff search runs ($numSingleSearchesDiff single searches) done in last $minutes minutes. Total search runs: $numSearchesTotal")
      log.info(s"DecisionContext: Ticker stats: $tickerStats, Orderbook stats: $orderBookStats")
      lastLifeSigh = Instant.now()
      numSingleSearchesDiff = 0
      numSearchesDiff = 0
    }
  }

  log.info("FooTrader up and running ...")

  override def receive: Receive = {
    // from Scheduler

    case Trigger() =>
      if (pendingOrderBundles.size < maxOpenOrderBundles) {
        tradeRoom ! GetTradeDecisionContext()
      }

    // response from TradeRoom

    case dc: TradeDecisionContext =>
      log.debug(s"Using TradeDecisionContext: with Tickers for Tradepairs[${dc.tickers.keys}], OrderBooks for TradePairs[${dc.orderBooks.keys}], etc.")
      lifeSign(dc)
      numSearchesDiff += 1
      numSearchesTotal += 1
      findBestShot(dc) match {
        case Some(orderBundle) =>
          // TODO pendingOrderBundles += (orderBundle.id -> orderBundle)
          tradeRoom ! orderBundle
        case None =>
      }

    case OrderBundlePlaced(orderBundleId) =>
      val ob = pendingOrderBundles(orderBundleId)
      pendingOrderBundles = pendingOrderBundles - orderBundleId
      activeOrderBundles += (orderBundleId -> ob)
      log.info(s"FooTrader: $ob")

    case OrderBundleCompleted(ob) =>
      activeOrderBundles -= ob.orderBundle.id
      log.info(s"FooTrader: $ob")

    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
