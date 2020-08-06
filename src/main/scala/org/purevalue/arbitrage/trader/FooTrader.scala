package org.purevalue.arbitrage.trader

import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.TradePairDataManager.{AskPosition, BidPosition}
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

  val scheduleRate: FiniteDuration = FiniteDuration(config.getDuration("schedule-rate").toNanos, TimeUnit.NANOSECONDS)

  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(0.seconds, scheduleRate, self, Trigger())

  def newUUID(): UUID = UUID.randomUUID() // switch to Time based UUID when connecting a DB like cassandra

  def calculateWinUSDT(orders: Seq[Order], dc:TradeDecisionContext): Double = {
    val invoice: Seq[CryptoValue] = orders.flatMap(_.bill)
    invoice.map(_.convertTo(Asset("USDT"), dc)).sum
  }

  def convertToUSDT(amount:Double, asset:Asset, dc:TradeDecisionContext): Double =
    CryptoValue(asset, amount).convertTo(Asset("USDT"), dc)

  def findBestShot(tradePair: TradePair, dc: TradeDecisionContext): Option[OrderBundle] = {
    val minBalanceBeforeTradeInUSDT = config.getDouble("order-bundle.min-balance-before-trade-in-usdt")
    val whatsSpendablePerExchange: Map[String, Set[Asset]] =
      dc.walletPerExchange
        .map(pair => (
          pair._1,
          pair._2.assets
            .filter(a => convertToUSDT(a._2, a._1, dc) >= minBalanceBeforeTradeInUSDT)
            .keySet))

    val books4Buy = dc.orderBooks(tradePair).values
      .filter(b => whatsSpendablePerExchange(b.exchange).contains(b.tradePair.quoteAsset))
    val books4Sell = dc.orderBooks(tradePair).values
      .filter(b => whatsSpendablePerExchange(b.exchange).contains(b.tradePair.baseAsset))

    // safety check
    if (!books4Buy.forall(_.tradePair == tradePair)
          || !books4Sell.forall(_.tradePair == tradePair)) {
      throw new RuntimeException("safety-check failed")
    }

    val tradeQuantityUSDT = config.getDouble("order-bundle.trade-amount-in-usdt")

    val highestBid: Tuple2[String, BidPosition] = // that's what we try to buy
      books4Buy
        .map(e => (e.exchange, e.highestBid))
        .maxBy(_._2.price)
    val lowestAsk: Tuple2[String, AskPosition] = // that's what we try to sell
      books4Sell
      .map(e => (e.exchange, e.lowestAsk))
      .minBy(_._2.price)

    if (highestBid._1 == lowestAsk._1) {
      log.warn(s"${Emoji.SadAndConfused} found highest bid $highestBid and lowest ask $lowestAsk on the same exchange.")
      return None
    }

    val orderBundleId = newUUID()
    val orderLimitAdditionPct:Double = config.getDouble("order-bundle.order-limit-addition-percentage")
    val amountBaseAsset = CryptoValue(Asset("USDT"), tradeQuantityUSDT).convertTo(tradePair.baseAsset, dc)
    val ourBuyBaseAssetOrder = Order(
      newUUID(),
      orderBundleId,
      lowestAsk._1,
      tradePair,
      TradeDirection.Buy,
      dc.feePerExchange(lowestAsk._1),
      Some(amountBaseAsset),
      None,
      lowestAsk._2.price * (1.0d + orderLimitAdditionPct*0.01d))

    val amountQuoteAsset = CryptoValue(Asset("USDT"), tradeQuantityUSDT).convertTo(tradePair.quoteAsset, dc)
    val ourSellBaseAssetOrder = Order(
      newUUID(),
      orderBundleId,
      highestBid._1,
      tradePair,
      TradeDirection.Sell,
      dc.feePerExchange(highestBid._1),
      None,
      Some(amountQuoteAsset),
      highestBid._2.price * (1.0d - orderLimitAdditionPct*0.01d)
    )

    val estimatedWinUSDT: Double = calculateWinUSDT(Seq(ourBuyBaseAssetOrder, ourSellBaseAssetOrder), dc)
    if (estimatedWinUSDT >= config.getDouble("order-bundle.min-gain-in-usdt")) {
      Some(OrderBundle(
        orderBundleId,
        name,
        self,
        LocalDateTime.now(),
        List(ourBuyBaseAssetOrder, ourSellBaseAssetOrder),
        estimatedWinUSDT,
        ""
      ))
    } else {
      None
    }
  }

  def findBestShot(dc: TradeDecisionContext): Option[OrderBundle] = {
    var result: Option[OrderBundle] = None
    for (tradePair <- dc.orderBooks.keySet) {
      findBestShot(tradePair, dc) match {
        case Some(shot) => if (result.isEmpty || shot.estimatedWinUSDT > result.get.estimatedWinUSDT) {
          result = Some(shot)
        }
        case None =>
      }
    }
    result
  }

  override def receive: Receive = {
    // from Scheduler

    case Trigger =>
      if (pendingOrderBundles.size < maxOpenOrderBundles) {
        tradeRoom ! GetTradeDecisionContext()
      }

    // response from TradeRoom

    case t: TradeDecisionContext =>
      findBestShot(t) match {
        case Some(orderBundle) =>
          pendingOrderBundles += (orderBundle.id -> orderBundle)
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
