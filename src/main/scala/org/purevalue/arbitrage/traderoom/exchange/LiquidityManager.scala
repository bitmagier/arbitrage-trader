package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.{NoSuchElementException, UUID}

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.traderoom.Asset.USDT
import org.purevalue.arbitrage.traderoom.TradeRoom.{LiquidityTransformationOrder, LiquidityTx}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager._
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.{ExchangeConfig, LiquidityManagerConfig}
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

object LiquidityManager {

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
  case class HouseKeeping()

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

  def props(config: LiquidityManagerConfig,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            tpData: ExchangePublicDataReadonly,
            wallet: Wallet,
            tradeRoom: ActorRef,
            findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
            referenceTicker: () => collection.Map[TradePair, Ticker]
           ): Props =
    Props(new LiquidityManager(config, exchangeConfig, tradePairs, tpData, wallet, tradeRoom, findOpenLiquidityTx, referenceTicker))
}
class LiquidityManager(val config: LiquidityManagerConfig,
                       val exchangeConfig: ExchangeConfig,
                       val tradePairs: Set[TradePair],
                       val tpData: ExchangePublicDataReadonly,
                       val wallet: Wallet,
                       val tradeRoom: ActorRef,
                       val findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
                       val referenceTicker: () => collection.Map[TradePair, Ticker]
                      ) extends Actor {

  private val log = LoggerFactory.getLogger(classOf[LiquidityManager])
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  var houseKeepingSchedule: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(1.minute, 30.seconds, self, HouseKeeping())
  var shutdownInitiated: Boolean = false


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
    val limit = Instant.now.minus(config.liquidityDemandActiveTime)
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
    val limit: Instant = Instant.now.minus(config.liquidityLockMaxLifetime)
    liquidityLocks = liquidityLocks.filter(_._2.createTime.isAfter(limit))
  }

  def determineUnlockedBalance: Map[Asset, Double] = {
    val lockedLiquidity: Map[Asset, Double] = liquidityLocks
      .values
      .flatMap(_.coins) // all locked values
      .groupBy(_.asset) // group by asset
      .map(e => (e._1, e._2.map(_.amount).sum)) // sum up values of same asset

    wallet.balance
      .filterNot(_._1.isFiat)
      .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
      .map(e => (e._1, Math.max(0.0, e._2.amountAvailable - lockedLiquidity.getOrElse(e._1, 0.0)))
      )
  }

  // Always refreshing demand for that coin.
  // Accept, if free (not locked) coins are available.
  def lockLiquidity(r: LiquidityRequest): Option[LiquidityLock] = {
    if (exchangeConfig.doNotTouchTheseAssets.intersect(r.coins).nonEmpty) throw new IllegalArgumentException
    if (r.coins.exists(_.asset.isFiat)) throw new IllegalArgumentException

    noticeDemand(LiquidityDemand(r)) // we always notice/refresh the demand, when 'someone' wants to lock liquidity

    val unlockedBalances: Map[Asset, Double] = determineUnlockedBalance
    val sumCoinsPerAsset = r.coins // coins should contain already only values of different assets, but we need to be 100% sure, that we do not work with multiple requests for the same coin
      .groupBy(_.asset)
      .map(x => CryptoValue(x._1, x._2.map(_.amount).sum))

    if (sumCoinsPerAsset.forall(c => unlockedBalances.getOrElse(c.asset, 0.0) >= c.amount)) {
      val lock = LiquidityLock(r.exchange, r.id, r.coins, Instant.now)
      addLock(lock)
      Some(lock)
    } else {
      None
    }
  }

  def checkValidity(r: LiquidityRequest): Unit = {
    if (r.exchange != exchangeConfig.exchangeName) throw new IllegalArgumentException
    if (r.coins.exists(c => exchangeConfig.doNotTouchTheseAssets.contains(c.asset))) throw new IllegalArgumentException("liquidity request for a DO-NOT-TOUCH asset")
  }

  /**
   * Calculates manufacturing costs for the demand in the reserveAsset unit
   */
  def estimatedManufactingCosts(demand: UniqueDemand, reserveAsset: Asset): Double = {
    CryptoValue(demand.asset, demand.amount)
      .convertTo(reserveAsset, referenceTicker())
      .amount
  }

  /**
   * Gives back a rating for trading a amount on that tradepair on the local exchange - compared to reference ticker
   * Ratings can be interpreted as a percentage of the local limit needed to fulfill a local trade compared to the reference ticker.
   * A result rate > 0 indicates a positive rating - being better than the reference ticker, a negative rate the opposite
   */
  def localExchangeRateRating(tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset:Double): Double = {
    val localLimit = determineGoodLimit(tradePair, tradeSide, amountBaseAsset)
    val referenceTickerPrice: Option[Double] = referenceTicker().get(tradePair).map(_.priceEstimate)
    referenceTickerPrice match {
      case Some(referencePrice) => tradeSide match {
        case TradeSide.Buy => 1.0 - localLimit / referencePrice // 1 - x/R
        case TradeSide.Sell => localLimit / referencePrice - 1.0 // x/R - 1
      }
      case None => 0.0 // we can not judge it, because no reference ticker available
    }
  }


  def determineLimitBasedOnTicker(ticker: Ticker, tradeSide: TradeSide): Double = {
    tradeSide match {
      case TradeSide.Sell => ticker.highestBidPrice * (1.0 - config.txLimitBelowOrAboveBestBidOrAsk)
      case TradeSide.Buy => ticker.lowestAskPrice * (1.0 + config.txLimitBelowOrAboveBestBidOrAsk)
    }
  }



  def determineGoodLimit(tradePair: TradePair, tradeSide: TradeSide, amountBaseAssetEstimate: Double): Double = {
    var limit: Option[Double] = None
    if (tpData.orderBook.nonEmpty) {
      limit = tpData.orderBook(tradePair).determineOptimalOrderLimit(tradeSide, amountBaseAssetEstimate * 1.5) // add 50% extra to increase our chance to fill the order
    }
    limit match {
      case Some(limit) => limit
      case None => determineLimitBasedOnTicker(tpData.ticker(tradePair), tradeSide)
    }
  }

  /**
   * Try to find an order to place for satisfying the demanded value
   * If that order was found and placed, the expected incoming value is returned, otherwise None is returned
   */
  def tryToPlaceALiquidityProvidingOrder(demand: UniqueDemand): Option[CryptoValue] = {
    val bestReserveAssets: Option[Tuple2[Asset, Double]] =
      exchangeConfig.reserveAssets
        .filterNot(demand.dontUseTheseReserveAssets.contains)
        .filter(e => tradePairs.contains(TradePair(demand.asset, e)))
        .filter(e => estimatedManufactingCosts(demand, e) < wallet.balance.get(e).map(_.amountAvailable).getOrElse(0.0)) // enough balance available?
        .map(e => (e, localExchangeRateRating(TradePair(demand.asset, e), TradeSide.Buy, demand.amount))) // join local exchange rate rating
        .filter(_._2 >= -config.maxAcceptableExchangeRateLossVersusReferenceTicker) // local rate acceptable?
        .sortBy(e => e._2)
        .lastOption

    if (bestReserveAssets.isEmpty) {
      if (log.isTraceEnabled) log.trace(s"No good reserve asset found to satisfy $demand")
      None
    } else {
      if (log.isTraceEnabled) log.trace(s"Found best usable reserve asset: ${bestReserveAssets.get._1}, rating=${bestReserveAssets.get._2} for providing $demand")
      val orderAmount: Double = demand.amount * (1.0 + config.providingLiquidityExtra)
      val tradePair = TradePair(demand.asset, bestReserveAssets.get._1)
      val limit = determineGoodLimit(tradePair, TradeSide.Buy, orderAmount)
      val orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeConfig.exchangeName, tradePair, TradeSide.Buy, exchangeConfig.fee, orderAmount, limit)

      tradeRoom ! LiquidityTransformationOrder(orderRequest)

      Some(CryptoValue(tradePair.baseAsset, orderAmount))
    }
  }

  // aim: free available liquidity shall be available to fulfill the sum of demand
  def provideDemandedLiquidity(): List[CryptoValue] = {
    val unlocked: Map[Asset, Double] = determineUnlockedBalance
    val unsatisfiedDemand: Iterable[UniqueDemand] =
      liquidityDemand.values
        .map(e =>
          UniqueDemand(
            e.tradePattern,
            e.asset,
            Math.max(0.0, e.amount - unlocked.getOrElse(e.asset, 0.0)),
            e.dontUseTheseReserveAssets,
            e.lastRequested)) // missing coins
        .filter(_.amount != 0.0)

    var incomingLiquidity: List[CryptoValue] = List()
    for (d: UniqueDemand <- unsatisfiedDemand) {
      tryToPlaceALiquidityProvidingOrder(d) match {
        case None => log.info(s"${Emoji.LookingDown}   Unable to provide liquidity demand: $d")
        case Some(incoming) => incomingLiquidity = incoming :: incomingLiquidity
      }
    }
    incomingLiquidity
  }

  def reserveAssetsWithLowLiquidity: Set[Asset] = {
    try {
      val fromWallet: Set[Asset] =
        wallet.balance
          .filter(e => exchangeConfig.reserveAssets.contains(e._1))
          .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
          .filterNot(_._1.isFiat)
          .filter(_._1.canConvertTo(USDT, tpData.ticker))
          .map(e => (e, CryptoValue(e._1, e._2.amountAvailable).convertTo(USDT, tpData.ticker))) // + USDT value - we expect we can convert a reserve asset to USDT always
          .filter(e => e._2.amount < config.minimumKeepReserveLiquidityPerAssetInUSDT) // value below minimum asset liquidity value
          .map(_._1._1)
          .toSet

      val remainingReserveAssets = exchangeConfig.reserveAssets.filter(fromWallet.contains)
      fromWallet ++ remainingReserveAssets
    } catch {
      case e: NoSuchElementException =>
        throw new RuntimeException(s"Sorry, can't work with reserve assets in ${exchangeConfig.exchangeName} wallet, which cannot be converted to USDT. This is the wallet: $wallet")
    }
  }

  //  [Concept Reserve-Liquidity-Management] (copied from description above)
  //  - Unused (not locked or demanded) liquidity of a non-Reserve-Asset will be automatically converted to a Reserve-Asset.
  //    Which reserve-asset it will be, is determined by:
  //    - [non-loss-asset-filter] Filtering acceptable exchange-rates based on ticker on that exchange compared to a [ReferenceTicker]-value
  //    - [fill-up] Try to reach minimum configured balance of each reserve-assets in their order of preference
  //    - [play safe] Remaining value goes to first (highest prio) reserve-asset (having a acceptable exchange-rate)
  def convertBackToReserveAsset(coins: CryptoValue): Option[CryptoValue] = {
    val possibleReserveAssets: List[Tuple2[Asset, Double]] =
      exchangeConfig.reserveAssets
        .filter(e => tpData.ticker.contains(TradePair(coins.asset, e))) // filter available TradePairs only (with a local ticker)
        .map(e => (e, localExchangeRateRating(TradePair(coins.asset, e), TradeSide.Sell, coins.amount))) // add local exchange rate rating

    val availableReserveAssets: List[Tuple2[Asset, Double]] =
      possibleReserveAssets
        .filter(_._2 >= -config.maxAcceptableExchangeRateLossVersusReferenceTicker) // [non-loss-asset-filter]

    if (availableReserveAssets.isEmpty) {
      if (possibleReserveAssets.isEmpty) log.debug(s"No reserve asset available to convert back $coins")
      else log.debug(s"Currently no reserve asset available with a good exchange-rate to convert back $coins. " +
        s"Available assets/ticker-rating: $availableReserveAssets")
      return None
    }

    val bestAvailableReserveAssets: List[Asset] =
      availableReserveAssets
        .sortBy(e => e._2) // sorted by local exchange rate rating
        .map(_._1) // asset only
        .reverse // best rating first

    val reserveAssetsNeedFillUp: Set[Asset] = reserveAssetsWithLowLiquidity
    var destinationReserveAsset: Option[Asset] = bestAvailableReserveAssets.find(reserveAssetsNeedFillUp.contains) // [fill-up]
    if (destinationReserveAsset.isDefined) {
      log.info(s"${Emoji.ThreeBitcoin}  ${exchangeConfig.exchangeName}: transferring $coins back to reserve asset ${destinationReserveAsset.get} [fill-up]")
    }

    if (destinationReserveAsset.isEmpty) {
      destinationReserveAsset = Some(availableReserveAssets.head._1) // [play safe]
      log.info(s"${Emoji.ThreeBitcoin}  ${exchangeConfig.exchangeName}: transferring $coins back to reserve asset ${destinationReserveAsset.get} [primary sink]")
    }

    val tradePair = TradePair(coins.asset, destinationReserveAsset.get)
    val limit: Double = determineGoodLimit(tradePair, TradeSide.Sell, coins.amount)
    val orderRequest = OrderRequest(
      UUID.randomUUID(),
      None,
      exchangeConfig.exchangeName,
      tradePair,
      TradeSide.Sell,
      exchangeConfig.fee,
      coins.amount,
      limit
    )

    tradeRoom ! LiquidityTransformationOrder(orderRequest)

    Some(CryptoValue(destinationReserveAsset.get, limit * coins.amount))
  }

  def convertBackLiquidityTxActive(source: Asset): Boolean = {
    findOpenLiquidityTx(e =>
      (e.orderRequest.tradeSide == TradeSide.Sell
        && e.orderRequest.tradePair.baseAsset == source
        && exchangeConfig.reserveAssets.contains(e.orderRequest.tradePair.quoteAsset)) ||
        (e.orderRequest.tradeSide == TradeSide.Buy
          && e.orderRequest.tradePair.quoteAsset == source
          && exchangeConfig.reserveAssets.contains(e.orderRequest.tradePair.baseAsset)))
      .isDefined
  }

  def convertBackNotNeededNoneReserveAssetLiquidity(): List[CryptoValue] = {
    val freeUnusedNoneReserveAssetLiquidity: Map[Asset, Double] =
      determineUnlockedBalance
        .filterNot(e => exchangeConfig.reserveAssets.contains(e._1)) // only select non-reserve-assets
        .filterNot(e => convertBackLiquidityTxActive(e._1)) // don't issue another liquidity tx for an asset, where another tx one is still active
        .map(e => ( // reduced by demand for that asset
          e._1,
          Math.max(
            0.0,
            e._2 - liquidityDemand.values
              .filter(_.asset == e._1)
              .map(_.amount)
              .sum
          )))
        .filter(_._2 > 0.0) // remove empty values
        .filter(_._1.canConvertTo(USDT, tpData.ticker))
        .map(e => (e._1, e._2, CryptoValue(e._1, e._2).convertTo(USDT, tpData.ticker))) // join USDT value
        .filter(e => e._3.amount >= config.dustLevelInUsdt) // ignore values below the dust level
        .map(e => (e._1, e._2))
        .toMap

    var incomingLiquidity: List[CryptoValue] = List()
    for (f <- freeUnusedNoneReserveAssetLiquidity) {
      incomingLiquidity = convertBackToReserveAsset(CryptoValue(f._1, f._2)).toList ::: incomingLiquidity
    }
    incomingLiquidity
  }

  def liquidityConversionPossibleBetween(a: Asset, b: Asset): Boolean = {
    tradePairs.contains(TradePair(a, b)) || tradePairs.contains(TradePair(b, a))
  }

  def rebalanceReserveAssetsAmountOrders(pendingIncomingReserveLiquidity: List[CryptoValue]): List[OrderRequest] = {
    log.trace(s"re-balancing reserve asset wallet:${wallet.balance} with pending incoming $pendingIncomingReserveLiquidity")
    val currentReserveAssetsBalance: List[CryptoValue] = wallet.balance
      .filter(e => exchangeConfig.reserveAssets.contains(e._1))
      .map(e => CryptoValue(e._1, e._2.amountAvailable))
      .toList :::
      exchangeConfig.reserveAssets // add zero balance for reserve assets not contained in wallet
        .filterNot(wallet.balance.keySet.contains)
        .map(e => CryptoValue(e, 0.0)) // so consider not delivered, empty balances too

    val virtualReserveAssetsAggregated: Iterable[CryptoValue] =
      (pendingIncomingReserveLiquidity ::: currentReserveAssetsBalance)
        .groupBy(_.asset)
        .map(e => CryptoValue(e._1, e._2.map(_.amount).sum))

    val unableToConvert = virtualReserveAssetsAggregated.filter(e => !e.canConvertTo(USDT, tpData.ticker))
    if (unableToConvert.nonEmpty) {
      log.warn(s"${Emoji.EyeRoll}  Some reserve assets cannot be judged, because we cannot convert them to USDT, so no liquidity transformation is possible for them: $unableToConvert")
    }

    // a bucket is a portion of an reserve asset having a specific value (value is configured in 'rebalance-tx-granularity-in-usdt')

    // all reserve assets, that need more value : Map(Asset -> liquidity buckets missing)
    var liquiditySinkBuckets: Map[Asset, Int] = virtualReserveAssetsAggregated
      .filter(_.canConvertTo(USDT, tpData.ticker))
      .map(e => (e.asset, e.convertTo(USDT, referenceTicker()).amount)) // amount in USDT
      .filter(_._2 < config.minimumKeepReserveLiquidityPerAssetInUSDT) // below min keep amount?
      .map(e => (e._1, ((config.minimumKeepReserveLiquidityPerAssetInUSDT - e._2) / config.rebalanceTxGranularityInUSDT).ceil.toInt)) // buckets needed
      .toMap
    // all reserve assets, that have extra liquidity to distribute : Map(Asset -> liquidity buckets available for distribution)
    var liquiditySourcesBuckets: Map[Asset, Int] = currentReserveAssetsBalance // we can take coin only from currently really existing balance
      .filter(_.canConvertTo(USDT, tpData.ticker))
      .map(e => (e.asset, e.convertTo(USDT, referenceTicker()).amount)) // amount in USDT
      .filter(_._2 >= config.minimumKeepReserveLiquidityPerAssetInUSDT + config.rebalanceTxGranularityInUSDT) // having more than the minimum limit + tx-granularity
      .map(e => (e._1, ((e._2 - config.minimumKeepReserveLiquidityPerAssetInUSDT) / config.rebalanceTxGranularityInUSDT).floor.toInt)) // buckets to provide
      .toMap

    def removeBuckets(liquidityBuckets: Map[Asset, Int], asset: Asset, numBuckets: Int): Map[Asset, Int] = {
      // asset or numBuckets may not exist
      liquidityBuckets.map {
        case (k, v) if k == asset => (k, Math.max(0, v - numBuckets)) // don't go below zero when existing number of buckets is smaller that numBuckets to remove (may happen for DefaultSink)
        case (k, v) => (k, v)
      }.filterNot(_._2 == 0)
    }

    var liquidityTransactions: List[OrderRequest] = Nil

    // try to satisfy all liquiditySinks using available sources
    // if multiple sources are available, we take from least priority reserve assets first

    var weAreDoneHere: Boolean = false
    while (liquiditySourcesBuckets.nonEmpty && !weAreDoneHere) {
      log.trace(s"Re-balance reserve assets: sources: $liquiditySourcesBuckets / sinks: $liquiditySinkBuckets")
      import util.control.Breaks._
      breakable {
        val DefaultSink = exchangeConfig.reserveAssets.head
        val sinkAsset: Asset = exchangeConfig.reserveAssets
          .find(liquiditySinkBuckets.keySet.contains) // highest priority reserve asset [unsatisfied] sinks first
          .getOrElse(DefaultSink) // or DefaultSink

        val sourceAsset: Option[Asset] = exchangeConfig.reserveAssets
          .reverse
          .filter(a => liquidityConversionPossibleBetween(a, sinkAsset))
          .find(liquiditySourcesBuckets.keySet.contains) // lowest priority reserve asset with liquidity to provide

        if (sourceAsset.isEmpty) {
          // no conversion possible between existing sourceBuckets and sink
          // => skip sinkBuckets
          if (liquiditySinkBuckets.contains(sinkAsset)) {
            liquiditySinkBuckets = liquiditySinkBuckets - sinkAsset
            break
          } else {
            // here we have the case where the (last-option) DefaultSink is the only available one, but no source can satisfy it
            weAreDoneHere = true
            break
          }
        }

        val bucketsToTransfer =
          if (liquiditySinkBuckets.contains(sinkAsset)) { // all good case
            Math.min(liquiditySinkBuckets(sinkAsset), liquiditySourcesBuckets(sourceAsset.get))
          } else { // DefaultSink as last option case
            liquiditySourcesBuckets(sourceAsset.get)
          }

        val tx: OrderRequest =
          TradePair(sinkAsset, sourceAsset.get) match {

            case tp if tpData.ticker.contains(tp) =>
              val tradePair = tp
              val tradeSide = TradeSide.Buy
              val baseAssetBucketValue: Double = CryptoValue(USDT, config.rebalanceTxGranularityInUSDT).convertTo(tradePair.baseAsset, tpData.ticker).amount
              val orderAmountBaseAsset = bucketsToTransfer * baseAssetBucketValue
              val limit = determineGoodLimit(tradePair, tradeSide, orderAmountBaseAsset)
              OrderRequest(UUID.randomUUID(), None, exchangeConfig.exchangeName, tradePair, tradeSide, exchangeConfig.fee, orderAmountBaseAsset, limit)

            case tp if tpData.ticker.contains(tp.reverse) =>
              val tradePair = tp.reverse
              val tradeSide = TradeSide.Sell
              val quoteAssetBucketValue: Double = CryptoValue(USDT, config.rebalanceTxGranularityInUSDT).convertTo(tradePair.quoteAsset, tpData.ticker).amount
              val amountBaseAssetEstimate = bucketsToTransfer * quoteAssetBucketValue / tpData.ticker(tradePair).priceEstimate
              val limit = determineGoodLimit(tradePair, tradeSide, amountBaseAssetEstimate)
              val orderAmountBaseAsset = bucketsToTransfer * quoteAssetBucketValue / limit
              OrderRequest(UUID.randomUUID(), None, exchangeConfig.exchangeName, tradePair, tradeSide, exchangeConfig.fee, orderAmountBaseAsset, limit)

            case _ => throw new RuntimeException(s"${exchangeConfig.exchangeName}: No local tradepair found to convert $sourceAsset to $sinkAsset")
          }

        liquidityTransactions = tx :: liquidityTransactions

        liquiditySinkBuckets = removeBuckets(liquiditySinkBuckets, sinkAsset, bucketsToTransfer)
        liquiditySourcesBuckets = removeBuckets(liquiditySourcesBuckets, sourceAsset.get, bucketsToTransfer)
      }
    }

    log.trace(s"Re-balance (unsquashed) tx orders:\n${liquidityTransactions.mkString("\n")}\n Remaining sinks: $liquiditySinkBuckets")
    // merge possible splitted orders towards primary reserve asset
    liquidityTransactions = liquidityTransactions
      .groupBy(e => (e.tradePair, e.tradeSide))
      .map { e =>
        val f = e._2.head
        val amount = e._2.map(_.amountBaseAsset).sum
        OrderRequest(f.id, f.orderBundleId, f.exchange, f.tradePair, f.tradeSide, f.fee, amount, f.limit)
      }.toList

    if (liquidityTransactions.nonEmpty) {
      log.info(s"${Emoji.ThreeBitcoin}  ${exchangeConfig.exchangeName}: re-balance reserve assets: $virtualReserveAssetsAggregated " +
        s"with following transactions:\n${liquidityTransactions.mkString("\n")}")
    }
    liquidityTransactions
  }


  def liquidityRequest(r: LiquidityRequest): Unit = {
    if (shutdownInitiated) sender ! None
    else {
      checkValidity(r)
      sender() ! lockLiquidity(r)
      self ! HouseKeeping()
    }
  }

  // Management takes care, that we follow the liquidity providing & back-converting strategy (described above)
  def houseKeeping(): Unit = {
    if (shutdownInitiated) return

    clearObsoleteLocks()
    clearObsoleteDemands()
    val demanded: List[CryptoValue] = provideDemandedLiquidity()
    val incomingReserveLiquidity = convertBackNotNeededNoneReserveAssetLiquidity()
    val totalIncomingReserveLiquidity = incomingReserveLiquidity ::: demanded.filter(e => exchangeConfig.reserveAssets.contains(e.asset))

    rebalanceReserveAssetsAmountOrders(totalIncomingReserveLiquidity).foreach { o =>
      tradeRoom ! LiquidityTransformationOrder(o)
    }
  }

  def stop(): Unit = {
    shutdownInitiated = true
    houseKeepingSchedule.cancel()
  }

  override def receive: Receive = {
    // @formatter:off
    case r: LiquidityRequest        => liquidityRequest(r)
    case LiquidityLockClearance(id) => clearLock(id)
    case HouseKeeping()             => houseKeeping()
    case TradeRoom.Stop(_)          => stop()
    // @formatter:off
  }
}

// TODO statistics: min/max/average time a liquidity providing order needs to be filled
// TODO statistics: LiquidityRequest successful ones / unsuccessful ones
// TODO statistics: min/max/average time a convert back to reserve-liquidity tx needs to be filled
// TODO statistics: total number and final balance of all done liquidity providing and back-converting transactions