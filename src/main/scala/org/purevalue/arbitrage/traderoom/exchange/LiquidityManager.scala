package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props}
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.traderoom.TradeRoom.{FinishedLiquidityTx, LiquidityTx}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager._
import org.purevalue.arbitrage.{Config, ExchangeConfig}
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

/*
- The LiquidityManager is responsible for providing the Assets which are requested by Traders.
     There is one manager per exchange, which:
        - manages liquidity storing assets (like BTC, USDT) (unused altcoin liquidity goes back to these)
        - provides/creates liquidity of specific assets demanded by (unfulfilled) liquidity requests, which is believed to be used soon by next upcoming trades

   [Concept]
   - Every single valid OrderRequest (no matter if enough balance is available or not) will result in a Liquidity-Request,
     which may be granted or not, based on the available (yet unreserved) asset balance
   - If that Liquidity-Request is covered by the current balance of the corresponding wallet,
       then it is granted and this amount in the Wallet is marked as locked (for a limited duration until clearance) => [LiquidityLock]
   - Else, if that Liquidity-Request is not covered by the current balance, then
       - it is denied and a LiquidityDemand is noticed by the ExchangeLiquidityManager,
         which (may) result in a liquidity providing trade in favor of the requested asset balance, given that enough Reserve-Liquidity is available.

   - Every completed trade (no matter if succeeded or canceled) will result in a clearance of it's previously acquired Liquidity-Locks,
   - Clearance of a Liquidity-Lock means removal of that lock (the underlying coins should be gone into a transaction in between anyway):
   - In case, that the maximum lifetime of a liquidity-lock is reached, it will be cleared automatically by housekeeping

   [Concept Liquidity-Demand]
     - The Liquidity-Demand can only be fulfilled, when enough amount of one of the configured Reserve-Assets is available
     - Furthermore it shall only be fulfilled by a Reserve-Asset, which is not involved in any order of the connected OrderRequestBundle, where the demand comes from!
     - Furthermore it can only be fulfilled, if the current exchange-rate on the local exchange is good enough,
       which means, it must be close to the Reference-Ticker exchange-rate or better than that (getting more coins out of the same amount of reserve-asset)

   [Concept Reserve-Liquidity-Management]
     - Unused (not locked or demanded) liquidity of a non-Reserve-Asset will be automatically converted to a Reserve-Asset.
       Which reserve-asset it will be, is determined by:
       - [non-loss-asset-filter] Filtering acceptable exchange-rates based on ticker on that exchange compared to a [ReferenceTicker]-value
       - [fill-up] Try to reach minimum configured balance of each reserve-assets in their order of preference
       - [play safe] Remaining value goes to first (highest prio) reserve-asset (having a acceptable exchange-rate)
*/

object LiquidityManager {
  /**
   * UniqueDemand
   * A unique demand is characterized by an asset name and a trade-pattern.
   * The trade pattern is a type identifier for incoming orderbundles typically containing: Trader-name + trading strategy name (optional)
   * The point is, that demand for a coin on an exchange with the same trade-pattern are condensed to the last one,
   * while demands for the same coin with different trade-pattern are added up.
   */
  case class UniqueDemand(tradePattern: String, // UK (different trader or different trading strategy shall be represented by a different tradePattern)
                          asset: Asset, // UK
                          amount: Double,
                          dontUseTheseReserveAssets: Set[Asset],
                          lastRequested: Instant) {
    def getUk: String = tradePattern + asset.officialSymbol
  }

  case class HouseKeeping()

  case class LiquidityRequest(id: UUID,
                              createTime: Instant,
                              exchange: String,
                              tradePattern: String,
                              coins: Seq[CryptoValue],
                              dontUseTheseReserveAssets: Set[Asset])
  case class LiquidityLock(exchange: String,
                           liquidityRequestId: UUID,
                           coins: Seq[CryptoValue],
                           createTime: Instant)
  case class LiquidityLockClearance(liquidityRequestId: UUID)

  class OrderBookTooFlatException(val tradePair: TradePair, val side: TradeSide) extends Exception

  def determineRealisticLimit(orderBook: collection.Map[TradePair, OrderBook],
                              ticker: collection.Map[TradePair, Ticker],
                              tradePair: TradePair,
                              tradeSide: TradeSide,
                              amountBaseAsset: Double,
                              txLimitAwayFromEdgeLimit: Double): Double = {
    new OrderLimitChooser(orderBook.get(tradePair), ticker(tradePair))
      .determineRealisticOrderLimit(tradeSide, amountBaseAsset, txLimitAwayFromEdgeLimit) match {
      case Some(limit) => limit
      case None => throw new OrderBookTooFlatException(tradePair, tradeSide)
    }
  }

  /**
   * Gives back a rating for trading a amount on that tradepair on the local exchange - compared to reference ticker
   * Ratings can be interpreted as a percentage of the local limit needed to fulfill a local trade compared to the reference ticker.
   * A result rate > 0 indicates a positive rating - being better than the reference ticker, a negative rate the opposite
   */
  def localExchangeRateRating(orderBook: collection.Map[TradePair, OrderBook],
                              ticker: collection.Map[TradePair, Ticker],
                              referenceTicker: collection.Map[TradePair, Ticker],
                              tradePair: TradePair, tradeSide:
                              TradeSide, amountBaseAsset: Double,
                              txLimitAwayFromEdgeLimit: Double): Double = {
    val localLimit: Double = determineRealisticLimit(orderBook, ticker, tradePair, tradeSide, amountBaseAsset, txLimitAwayFromEdgeLimit)
    val referenceTickerPrice: Option[Double] = referenceTicker.get(tradePair).map(_.priceEstimate)
    referenceTickerPrice match {
      case Some(referencePrice) => tradeSide match {
        case TradeSide.Buy => 1.0 - localLimit / referencePrice // 1 - x/R
        case TradeSide.Sell => localLimit / referencePrice - 1.0 // x/R - 1
      }
      case None => 0.0 // we can not judge it, because no reference ticker available
    }
  }

  def determineUnlockedBalance(balance: Map[Asset, Balance],
                               liquidityLocks: Map[UUID, LiquidityLock],
                               exchangeConfig: ExchangeConfig): Map[Asset, Double] = {
    val lockedLiquidity: Map[Asset, Double] = liquidityLocks
      .values
      .flatMap(_.coins) // all locked values
      .groupBy(_.asset) // group by asset
      .map(e => (e._1, e._2.map(_.amount).sum)) // sum up values of same asset

    balance
      .filterNot(_._1.isFiat)
      .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
      .map(e => (e._1, Math.max(0.0, e._2.amountAvailable - lockedLiquidity.getOrElse(e._1, 0.0)))
      )
  }


  def props(config: Config,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            tpData: ExchangePublicDataReadonly,
            wallet: Wallet,
            tradeRoom: ActorRef,
            findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
            findFinishedLiquidityTx: (FinishedLiquidityTx => Boolean) => Option[FinishedLiquidityTx],
            referenceTicker: () => collection.Map[TradePair, Ticker]
           ): Props =
    Props(new LiquidityManager(config, exchangeConfig, tradePairs, tpData, wallet, tradeRoom, findOpenLiquidityTx, findFinishedLiquidityTx, referenceTicker))
}
class LiquidityManager(val config: Config,
                       val exchangeConfig: ExchangeConfig,
                       val tradePairs: Set[TradePair],
                       val publicData: ExchangePublicDataReadonly,
                       val wallet: Wallet,
                       val tradeRoom: ActorRef,
                       val findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
                       val findFinishedLiquidityTx: (FinishedLiquidityTx => Boolean) => Option[FinishedLiquidityTx],
                       val referenceTicker: () => collection.Map[TradePair, Ticker]
                      ) extends Actor {

  private case class LiquidityDemand(exchange: String,
                                     tradePattern: String,
                                     coins: Seq[CryptoValue],
                                     dontUseTheseReserveAssets: Set[Asset]) {
    if (coins.exists(_.asset.isFiat)) throw new IllegalArgumentException("Seriously, you demand for Fiat Money?")
  }

  private object LiquidityDemand {
    def apply(r: LiquidityRequest): LiquidityDemand =
      LiquidityDemand(r.exchange, r.tradePattern, r.coins, r.dontUseTheseReserveAssets)
  }

  private val log = LoggerFactory.getLogger(classOf[LiquidityManager])
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  var houseKeepingSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(1.minute, 30.seconds, self, HouseKeeping())
  var liquidityBalancer: ActorRef = _
  var houseKeepingRunning: Boolean = false
  var shutdownInitiated: Boolean = false

  // Map(uk:"trade-pattern + asset", UniqueDemand))
  var liquidityDemand: Map[String, UniqueDemand] = Map()
  var liquidityLocks: Map[UUID, LiquidityLock] = Map()

  def noticeUniqueDemand(d: UniqueDemand): Unit = {
    if (log.isTraceEnabled) log.trace(s"noticed $d")
    liquidityDemand = liquidityDemand + (d.getUk -> d)
  }

  def noticeDemand(d: LiquidityDemand): Unit = {
    d.coins
      .map(c => UniqueDemand(d.tradePattern, c.asset, c.amount, d.dontUseTheseReserveAssets, Instant.now))
      .foreach(noticeUniqueDemand)
  }

  def clearObsoleteDemands(): Unit = {
    val limit = Instant.now.minus(config.liquidityManager.liquidityDemandActiveTime)
    liquidityDemand = liquidityDemand.filter(_._2.lastRequested.isAfter(limit))
  }

  def clearLock(id: UUID): Unit = {
    liquidityLocks = liquidityLocks - id
    if (log.isTraceEnabled) log.trace(s"Liquidity lock with ID $id cleared")
    self ! HouseKeeping()
  }

  def addLock(l: LiquidityLock): Unit = {
    liquidityLocks = liquidityLocks + (l.liquidityRequestId -> l)
    if (log.isTraceEnabled) log.trace(s"Liquidity locked: $l")
  }

  def clearObsoleteLocks(): Unit = {
    val limit: Instant = Instant.now.minus(config.liquidityManager.liquidityLockMaxLifetime)
    liquidityLocks = liquidityLocks.filter(_._2.createTime.isAfter(limit))
  }

  // Accept, if free (not locked) coins are available.
  def lockLiquidity(r: LiquidityRequest): Option[LiquidityLock] = {
    if (exchangeConfig.doNotTouchTheseAssets.intersect(r.coins).nonEmpty) throw new IllegalArgumentException
    if (r.coins.exists(_.asset.isFiat)) throw new IllegalArgumentException

    val unlockedBalances: Map[Asset, Double] = determineUnlockedBalance(wallet.balance, liquidityLocks, exchangeConfig)
    val sumCoinsPerAsset = r.coins // coins should contain already only values of different assets, but we need to be 100% sure, that we do not work with multiple requests for the same coin
      .groupBy(_.asset)
      .map(x => CryptoValue(x._1, x._2.map(_.amount).sum))

    if (sumCoinsPerAsset.forall(c => unlockedBalances.getOrElse(c.asset, 0.0) >= c.amount)) {
      val lock = LiquidityLock(r.exchange, r.id, r.coins, Instant.now)
      addLock(lock)
      Some(lock)
    } else {
      log.debug(s"refused liquidity-lock request on ${r.exchange} for ${r.coins.mkString(" ,")} (don't use: ${r.dontUseTheseReserveAssets})")
      None
    }
  }

  def checkValidity(r: LiquidityRequest): Unit = {
    if (r.exchange != exchangeConfig.name) throw new IllegalArgumentException
    if (r.coins.exists(c => exchangeConfig.doNotTouchTheseAssets.contains(c.asset))) throw new IllegalArgumentException("liquidity request for a DO-NOT-TOUCH asset")
  }

  def liquidityRequest(r: LiquidityRequest): Unit = {
    if (shutdownInitiated) sender ! None
    else {
      checkValidity(r)
      noticeDemand(LiquidityDemand(r)) // notice/refresh the demand, when 'someone' wants to lock liquidity
      sender() ! lockLiquidity(r)
      self ! HouseKeeping()
    }
  }

  // Management takes care, that we follow the liquidity providing & back-converting strategy (described above)
  def houseKeeping(): Unit = {
    if (shutdownInitiated) return
    if (houseKeepingRunning) return
    houseKeepingRunning = true

    clearObsoleteLocks()
    clearObsoleteDemands()

    liquidityBalancer ! LiquidityBalancer.RunWithWorkingContext(
        wallet.balance.map(e => e._1 -> e._2).toMap,
        liquidityDemand,
        liquidityLocks)
  }

  override def preStart(): Unit = {
    liquidityBalancer = context.actorOf(LiquidityBalancer.props(config, exchangeConfig, tradePairs, publicData, findOpenLiquidityTx, findFinishedLiquidityTx, referenceTicker, tradeRoom),
      s"${exchangeConfig.name}-liquidity-manager-housekeeping")
  }

  def stop(): Unit = {
    shutdownInitiated = true
    houseKeepingSchedule.cancel()
  }

  override def receive: Receive = {
    // @formatter:off
    case r: LiquidityRequest          => liquidityRequest(r)
    case LiquidityLockClearance(id)   => clearLock(id)
    case HouseKeeping()               => houseKeeping()
    case LiquidityBalancer.Finished() => houseKeepingRunning = false
    case TradeRoom.Stop(_)            => stop()
    case Failure(e)                   => log.error("received failure", e); self ! PoisonPill
    // @formatter:off
  }
}

// TODO statistics: min/max/average time a liquidity providing order needs to be filled
// TODO statistics: LiquidityRequest successful ones / unsuccessful ones
// TODO statistics: min/max/average time a convert back to reserve-liquidity tx needs to be filled
// TODO statistics: total number and final balance of all done liquidity providing and back-converting transactions
