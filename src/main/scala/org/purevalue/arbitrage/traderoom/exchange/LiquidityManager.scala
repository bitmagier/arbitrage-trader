package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager._
import org.purevalue.arbitrage.{Config, ExchangeConfig}

import scala.concurrent.ExecutionContextExecutor

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
  def apply(config: Config,
            exchangeConfig: ExchangeConfig):
  Behavior[Command] =
    Behaviors.setup(context => new LiquidityManager(context, config, exchangeConfig))

  sealed trait Command extends Exchange.Message
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
                          dontUseTheseReserveAssets: Set[Asset], // not used any more; replaced by exchange rate rating
                          lastRequested: Instant) {
    def uk: String = tradePattern + asset.officialSymbol
  }

  case class GetState(replyTo: ActorRef[State]) extends Command
  case class State(liquidityDemand: Map[String, UniqueDemand],
                   liquidityLocks: Map[UUID, LiquidityLock])

  case class LiquidityLockRequest(id: UUID,
                                  createTime: Instant,
                                  exchange: String,
                                  tradePattern: String,
                                  coins: Seq[CryptoValue],
                                  isForLiquidityTx: Boolean,
                                  dontUseTheseReserveAssets: Set[Asset],
                                  wallet: Option[Wallet], // is filled in by exchange actor, which forwards this message
                                  replyTo: ActorRef[Option[LiquidityLock]]) extends Command {
    def withWallet(wallet: Wallet): LiquidityLockRequest = LiquidityLockRequest(
      id, createTime, exchange, tradePattern, coins, isForLiquidityTx, dontUseTheseReserveAssets, Some(wallet), replyTo
    )
  }

  case class LiquidityLock(exchange: String,
                           liquidityRequestId: UUID,
                           coins: Seq[CryptoValue],
                           createTime: Instant)
  case class LiquidityLockClearance(liquidityRequestId: UUID) extends Command

  class OrderBookTooFlatException(val tradePair: TradePair, val side: TradeSide) extends Exception
}
class LiquidityManager(context: ActorContext[Command],
                       config: Config,
                       exchangeConfig: ExchangeConfig
                      ) extends AbstractBehavior[Command](context) {

  import LiquidityManager._

  case class LiquidityDemand(exchange: String,
                             tradePattern: String,
                             coins: Seq[CryptoValue],
                             dontUseTheseReserveAssets: Set[Asset]) {
    if (coins.exists(_.asset.isFiat)) throw new IllegalArgumentException("Seriously, you demand for Fiat Money?")
  }

  private object LiquidityDemand {
    def apply(r: LiquidityLockRequest): LiquidityDemand =
      LiquidityDemand(r.exchange, r.tradePattern, r.coins, r.dontUseTheseReserveAssets)
  }

  private implicit val executionContext: ExecutionContextExecutor = actorSystem.executionContext

  private var shutdownInitiated: Boolean = false

  // Map(uk:"trade-pattern + asset", UniqueDemand))
  private var liquidityDemand: Map[String, UniqueDemand] = Map()
  private var liquidityLocks: Map[UUID, LiquidityLock] = Map()

  def noticeUniqueDemand(d: UniqueDemand): Unit = {
    if (context.log.isDebugEnabled) context.log.debug(s"noticed $d")
    liquidityDemand = liquidityDemand + (d.uk -> d)
  }

  def noticeDemand(d: LiquidityDemand): Unit = {
    d.coins
      .map(c => UniqueDemand(d.tradePattern, c.asset, c.amount, d.dontUseTheseReserveAssets, Instant.now))
      .foreach(noticeUniqueDemand)
  }

  def clearLock(id: UUID): Unit = {
    liquidityLocks = liquidityLocks - id
    if (context.log.isDebugEnabled) context.log.debug(s"Liquidity lock with ID $id cleared")
  }

  def addLock(l: LiquidityLock): Unit = {
    liquidityLocks = liquidityLocks + (l.liquidityRequestId -> l)
    if (context.log.isDebugEnabled) context.log.debug(s"Liquidity locked: $l")
  }

  def clearObsoleteDemands(): Unit = {
    val limit = Instant.now.minus(config.liquidityManager.liquidityDemandActiveTime)
    liquidityDemand = liquidityDemand.filter(_._2.lastRequested.isAfter(limit))
  }

  def clearObsoleteLocks(): Unit = {
    val limit: Instant = Instant.now.minus(config.liquidityManager.liquidityLockMaxLifetime)
    liquidityLocks = liquidityLocks.filter(_._2.createTime.isAfter(limit))
  }

  def determineUnlockedBalance(wallet: Wallet): Map[Asset, Double] = {
    val lockedLiquidity: Map[Asset, Double] = liquidityLocks
      .values
      .flatMap(_.coins) // all locked values
      .groupBy(_.asset) // group by asset
      .map(e => (e._1, e._2.map(_.amount).sum)) // sum up values of same asset

    wallet.balance
      .filterNot(_._1.isFiat)
      .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
      .map(e => (e._1, Math.max(0.0, e._2.amountAvailable - lockedLiquidity.getOrElse(e._1, 0.0))))
  }


  // just cleanup
  def houseKeeping(): Unit = {
    clearObsoleteLocks()
    clearObsoleteDemands()
  }

  // Accept, if free (not locked) coins are available.
  def lockLiquidity(r: LiquidityLockRequest): Option[LiquidityLock] = {
    if (r.coins.exists(e => exchangeConfig.doNotTouchTheseAssets.contains(e.asset))) throw new IllegalArgumentException
    if (r.coins.exists(_.asset.isFiat)) throw new IllegalArgumentException

    val unlockedBalances = determineUnlockedBalance(r.wallet.get)
    val sumCoinsPerAsset = r.coins // coins should contain already only values of different assets, but we need to be 100% sure, that we do not work with multiple requests for the same coin
      .groupBy(_.asset)
      .map(x => CryptoValue(x._1, x._2.map(_.amount).sum))

    if (sumCoinsPerAsset.forall(c => unlockedBalances.getOrElse(c.asset, 0.0) >= c.amount)) {
      val lock = LiquidityLock(r.exchange, r.id, r.coins, Instant.now)
      addLock(lock)
      Some(lock)
    } else {
      context.log.debug(s"refused liquidity-lock request on ${r.exchange} for ${r.coins.mkString(" ,")} (don't use: ${r.dontUseTheseReserveAssets})")
      None
    }
  }

  def checkValidity(r: LiquidityLockRequest): Unit = {
    if (r.exchange != exchangeConfig.name) throw new IllegalArgumentException
    if (r.coins.exists(c => exchangeConfig.doNotTouchTheseAssets.contains(c.asset))) throw new IllegalArgumentException("liquidity request for a DO-NOT-TOUCH asset")
  }

  def liquidityLockRequest(r: LiquidityLockRequest): Option[LiquidityLock] = {
    if (shutdownInitiated) None
    else {
      houseKeeping()
      checkValidity(r)
      if (!r.isForLiquidityTx) {
        noticeDemand(LiquidityDemand(r)) // notice/refresh the demand, when 'someone' wants to lock liquidity for trading
      }
      lockLiquidity(r)
    }
  }

  override def onMessage(message: Command): Behavior[Command] = {
    message match {
      // @formatter:off
      case r: LiquidityLockRequest      => r.replyTo ! liquidityLockRequest(r); this
      case LiquidityLockClearance(id)   => clearLock(id); this
      case GetState(replyTo)            => houseKeeping(); replyTo ! State(liquidityDemand, liquidityLocks); this
      // @formatter:off
    }
  }
}

// TODO statistics: min/max/average time a liquidity providing order needs to be filled
// TODO statistics: LiquidityRequest successful ones / unsuccessful ones
// TODO statistics: min/max/average time a convert back to reserve-liquidity tx needs to be filled
// TODO statistics: total number and final balance of all done liquidity providing and back-converting transactions
