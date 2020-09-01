package org.purevalue.arbitrage

import java.time.{Duration, Instant, LocalDateTime}
import java.util.UUID

import akka.actor.{Actor, Props}
import org.purevalue.arbitrage.ExchangeLiquidityManager.{LiquidityDemand, LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.TradeRoom.WalletUpdateTrigger
import org.slf4j.LoggerFactory

import scala.collection.Map

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
   - In case, that the maximum lifetime of a liquidity-lock is reached, it will be cleared automatically by the [TradeRoomSupervisor]

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

object ExchangeLiquidityManager {

  case class LiquidityRequest(id: UUID,
                              createTime: Instant,
                              exchange: String,
                              tradePattern:String,
                              coins: Seq[CryptoValue],
                              dontUseTheseReserveAssets: Set[Asset])
  case class LiquidityLock(exchange: String,
                           liquidityRequestId: UUID,
                           coins: Seq[CryptoValue],
                           createTime: Instant)
  case class LiquidityLockClearance(liquidityRequestId: UUID)
  case class LiquidityDemand(exchange: String,
                             tradePattern: String,
                             coins: Seq[CryptoValue],
                             dontUseTheseReserveAssets: Set[Asset])
  object LiquidityDemand {
    def apply(r: LiquidityRequest): LiquidityDemand =
      LiquidityDemand(r.exchange, r.tradePattern, r.coins, r.dontUseTheseReserveAssets)
  }

  def props(config: LiquidityManagerConfig, exchangeConfig: ExchangeConfig, wallet: Wallet): Props =
    Props(new ExchangeLiquidityManager(config, exchangeConfig, wallet))
}
class ExchangeLiquidityManager(config: LiquidityManagerConfig,
                               exchangeConfig: ExchangeConfig,
                               wallet: Wallet) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeLiquidityManager])

  val liquidityLockMaxLifetime: Duration = Duration.ofSeconds(10) // TODO config | When a liquidity lock is not cleared, this is the maximum time, it can stay active
  val liquidityDemandActiveTime: Duration = Duration.ofSeconds(60) // TODO config | When demanded liquidity is not requested within that time, the coins are transferred back to a reserve asset
  val providingAdditionalLiquidityAmountPercentage: Double = 1.0 // TODO config | We provide a little bit more than it was demanded, to potentially fulfill order requests with a slightly different amount

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
  // Map(asset -> Map(trade-pattern, UniqueDemand))
  var liquidityDemand: Map[String, UniqueDemand] = Map()

  def noticeUniqueDemand(d: UniqueDemand): Unit = {
    liquidityDemand = liquidityDemand + (d.getUk -> d)
  }

  def noticeDemand(d: LiquidityDemand): Unit = {
    d.coins
      .map(c => UniqueDemand(d.tradePattern, c.asset, c.amount, d.dontUseTheseReserveAssets, Instant.now))
      .foreach(noticeUniqueDemand)
    if (log.isTraceEnabled) log.trace(s"noticed $d")
  }

  def clearObsoleteDemands(): Unit = {
    liquidityDemand = liquidityDemand
  }

  var liquidityLocks: Map[UUID, LiquidityLock] = Map()

  def clearLock(id: UUID): Unit = {
    liquidityLocks = liquidityLocks - id
    if (log.isTraceEnabled) log.trace(s"Liquidity lock with ID $id cleared")
  }

  def addLock(l:LiquidityLock): Unit = {
    liquidityLocks = liquidityLocks + (l.liquidityRequestId -> l)
    if (log.isTraceEnabled) log.trace(s"Liquidity locked: $l")
  }

  def clearObsoleteLocks(): Unit = {
    val oldestValidTime: Instant = Instant.now.minus(liquidityLockMaxLifetime)
    liquidityLocks = liquidityLocks.filter(_._2.createTime.isAfter(oldestValidTime))
  }

  def determineFreeBalance: Map[Asset, Double] = {
    val lockedLiquidity: Map[Asset, Double] = liquidityLocks
      .values
      .flatMap(_.coins) // all locked values
      .groupBy(_.asset) // group by asset
      .map(e => (e._1, e._2.map(_.amount).sum)) // sum up values of same asset

    wallet.balance.map(e =>
      (e._1, Math.max(0.0, e._2.amountAvailable - lockedLiquidity.getOrElse(e._1, 0.0)))
    )
  }

  // Always refreshing demand for that coin.
  // Accept, if free (not locked) coins are available.
  def lockLiquidity(r: LiquidityRequest): Option[LiquidityLock] = {
    noticeDemand(LiquidityDemand(r)) // we always notice/refresh the demand, when 'someone' wants to lock liquidity

    val freeBalances: Map[Asset, Double] = determineFreeBalance
    val sumCoinsPerAsset = r.coins // coins should contain already only values of different assets, but we need to be 100% sure, that we do not work with multiple requests for the same coin
      .groupBy(_.asset)
      .map(x => CryptoValue(x._1, x._2.map(_.amount).sum))

    if (sumCoinsPerAsset.forall(c => freeBalances.getOrElse(c.asset, 0.0) >= c.amount)) {
      val lock = LiquidityLock(r.exchange, r.id, r.coins)
      addLock(lock)
      Some(lock)
    } else {
      None
    }
  }

  def checkValidity(r: LiquidityRequest): Unit = {
    if (r.exchange != exchangeConfig.exchangeName) throw new IllegalArgumentException
  }

  def placeLiquidityProvidingOrder(demand: UniqueDemand): CryptoValue = {

  }

  // aim: free available liquidity shall be available to fulfill the sum of demand
  def provideDemandedLiquidity(): List[CryptoValue] = {
    val free: Map[Asset, Double] = determineFreeBalance
    val unsatisfiedDemand: Iterable[UniqueDemand] =
      liquidityDemand.values
        .map(e => UniqueDemand(e.tradePattern, e.asset, Math.max(0.0, e.amount - free.getOrElse(e.asset, 0.0)), e.dontUseTheseReserveAssets, e.lastRequested)) // missing coins
        .filter(_.amount == 0.0)

    var willProvide: List[CryptoValue] = List()
    for (d: UniqueDemand <- unsatisfiedDemand) {
      placeLiquidityProvidingOrder(d)
    }
  }

  // Management takes care, that we follow the liquidity providing & back-converting strategy (described above)
  def callManagement(): Unit = {
    clearObsoleteLocks()
    clearObsoleteDemands()
    val demanded: List[CryptoValue] = provideDemandedLiquidity()
    val reserveLiquidityBackFlow: List[CryptoValue] = convertBackNotNeededNoneReserveAssetLiquidity()

    val reserveLiquidityComingSoon: List[CryptoValue] =
      reserveLiquidityBackFlow ::: demanded.filter(e => config.reserveAssets.contains(e.asset))
    balanceOutReserveAssetAmounts(reserveLiquidityComingSoon)
  }

  override def receive: Receive = {
    case r: LiquidityRequest =>
      checkValidity(r)
      sender() ! lockLiquidity(r)

    case LiquidityLockClearance(id) =>
      clearLock(id)

    case d: LiquidityDemand =>
      noticeDemand(d)
      callManagement()

    case WalletUpdateTrigger(_) =>
      callManagement()
  }
}
