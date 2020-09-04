package org.purevalue.arbitrage

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import org.purevalue.arbitrage.Asset.USDT
import org.purevalue.arbitrage.ExchangeLiquidityManager.{LiquidityDemand, LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.TradeRoom.{LiquidityTransformationOrder, ReferenceTicker, WalletUpdateTrigger}
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
                              tradePattern: String,
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

  def props(config: LiquidityManagerConfig, exchangeConfig: ExchangeConfig, tradeRoom: ActorRef, tpData: ExchangeTPData, wallet: Wallet, referenceTicker: ReferenceTicker): Props =
    Props(new ExchangeLiquidityManager(config, exchangeConfig, tradeRoom, tpData, wallet, referenceTicker))
}
class ExchangeLiquidityManager(config: LiquidityManagerConfig,
                               exchangeConfig: ExchangeConfig,
                               tradeRoom: ActorRef,
                               tpData: ExchangeTPData,
                               wallet: Wallet,
                               referenceTicker: ReferenceTicker) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeLiquidityManager])

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

  def addLock(l: LiquidityLock): Unit = {
    liquidityLocks = liquidityLocks + (l.liquidityRequestId -> l)
    if (log.isTraceEnabled) log.trace(s"Liquidity locked: $l")
  }

  def clearObsoleteLocks(): Unit = {
    val oldestValidTime: Instant = Instant.now.minus(config.liquidityLockMaxLifetime)
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
      val lock = LiquidityLock(r.exchange, r.id, r.coins, Instant.now)
      addLock(lock)
      Some(lock)
    } else {
      None
    }
  }

  def checkValidity(r: LiquidityRequest): Unit = {
    if (r.exchange != exchangeConfig.exchangeName) throw new IllegalArgumentException
  }


  /**
   * Calculates manufacturing costs for the demand in the reserveAsset unit
   */
  def estimatedManufactingCosts(demand: UniqueDemand, reserveAsset: Asset): Option[Double] = {
    CryptoValue(demand.asset, demand.amount)
      .convertTo(reserveAsset, tp => tpData.ticker.get(tp).map(_.priceEstimate))
      .map(_.amount)
  }

  /**
   * Gives back a rating of the local ticker, indicating how good the local exchange-rate is compared with the reference-ticker.
   * Ratings can be interpreted as a percentage being better (positive rating) or worse (negative rating) that the reference ticker.
   */
  def localTickerRating(tradePair: TradePair, tradeSide: TradeSide): Double = {
    tradeSide match {
      case TradeSide.Sell => tpData.ticker(tradePair).priceEstimate / referenceTicker.values(tradePair).currentPriceEstimate - 1.0 // x/R - 1
      case TradeSide.Buy => 1.0 - tpData.ticker(tradePair).priceEstimate / referenceTicker.values(tradePair).currentPriceEstimate // 1 - x/R
    }
  }

  // TODO fine-tune that algorithm using the order book data, if available
  def guessGoodLimit(tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double): Double = {
    tradeSide match {
      case TradeSide.Sell => tpData.ticker(tradePair).lowestAskPrice * (1.0 - config.txLimitBelowOrAboveBestBidOrAsk)
      case TradeSide.Buy => tpData.ticker(tradePair).lowestAskPrice * (1.0 + config.txLimitBelowOrAboveBestBidOrAsk)
    }
  }

  /**
   * Try to find an order to place for satisfying the demanded value
   * If that order was found and placed, the expected incoming value is returned, otherwise None is returned
   */
  def tryTpPlaceALiquidityProvidingOrder(demand: UniqueDemand): Option[CryptoValue] = {
    val bestReserveAssets: Option[Tuple2[Asset, Double]] =
      config.reserveAssets
        .filterNot(demand.dontUseTheseReserveAssets.contains)
        .filter(e => tpData.ticker.keySet.contains(TradePair.of(demand.asset, e))) // ticker available for trade pair?
        .filter(e => estimatedManufactingCosts(demand, e) match { // enough balance available in liquidity providing asset?
          case Some(costs) => wallet.balance.get(e).map(_.amountAvailable).getOrElse(0.0) >= costs
          case None => false
        })
        .map(e => (e, localTickerRating(TradePair.of(demand.asset, e), TradeSide.Buy)))
        .filter(_._2 >= -config.maxAcceptableLocalTickerLossFromReferenceTicker) // ticker rate acceptable?
        .sortBy(e => e._2)
        .lastOption

    if (bestReserveAssets.isEmpty) {
      if (log.isTraceEnabled) log.trace(s"No good reserve asset found to satisfy our demand")
      None
    } else {
      if (log.isTraceEnabled) log.trace(s"Found best usable reserve asset: ${bestReserveAssets.get._1}, rating=${bestReserveAssets.get._2}")
      val orderAmount: Double = demand.amount * (1.0 + config.providingLiquidityExtra)
      val tradePair = TradePair.of(demand.asset, bestReserveAssets.get._1)
      val limit = guessGoodLimit(tradePair, TradeSide.Buy, orderAmount)
      val orderRequest = OrderRequest(UUID.randomUUID(), null, exchangeConfig.exchangeName, tradePair, TradeSide.Buy,
        exchangeConfig.fee, orderAmount, limit)

      tradeRoom ! LiquidityTransformationOrder(orderRequest)

      Some(CryptoValue(tradePair.baseAsset, orderAmount))
    }
  }

  // aim: free available liquidity shall be available to fulfill the sum of demand
  def provideDemandedLiquidity(): List[CryptoValue] = {
    val free: Map[Asset, Double] = determineFreeBalance
    val unsatisfiedDemand: Iterable[UniqueDemand] =
      liquidityDemand.values
        .map(e =>
          UniqueDemand(
            e.tradePattern,
            e.asset,
            Math.max(0.0, e.amount - free.getOrElse(e.asset, 0.0)),
            e.dontUseTheseReserveAssets,
            e.lastRequested)) // missing coins
        .filter(_.amount != 0.0)

    var incomingLiquidity: List[CryptoValue] = List()
    for (d: UniqueDemand <- unsatisfiedDemand) {
      incomingLiquidity = tryTpPlaceALiquidityProvidingOrder(d).toList ::: incomingLiquidity
    }
    incomingLiquidity
  }


  def reserveAssetsWhichNeedFillUp: Set[Asset] = {
    try {
      wallet.balance
        .map(e => (e, CryptoValue(e._1, e._2.amountAvailable).convertTo(USDT, referenceTicker).get))
        .filter(e => e._2.amount < config.minimumKeepReserveLiquidityPerAssetInUSDT)
        .map(_._1._1)
        .toSet
    } catch {
      case e: NoSuchElementException =>
        log.error(s"ReferenceTicker is not containing all required exchange rates. ${e.getMessage}")
        Set()
    }
  }

  //  [Concept Reserve-Liquidity-Management] (copied from description above)
  //  - Unused (not locked or demanded) liquidity of a non-Reserve-Asset will be automatically converted to a Reserve-Asset.
  //    Which reserve-asset it will be, is determined by:
  //    - [non-loss-asset-filter] Filtering acceptable exchange-rates based on ticker on that exchange compared to a [ReferenceTicker]-value
  //    - [fill-up] Try to reach minimum configured balance of each reserve-assets in their order of preference
  //    - [play safe] Remaining value goes to first (highest prio) reserve-asset (having a acceptable exchange-rate)
  def convertBackToReserveAsset(coins: CryptoValue): Option[CryptoValue] = {
    val availableReserveAssets: List[Tuple2[Asset, Double]] =
      config.reserveAssets
        .map(e => (e, localTickerRating(TradePair.of(coins.asset, e), TradeSide.Sell))) // add rating
        .filter(e => e._2 >= -config.maxAcceptableLocalTickerLossFromReferenceTicker) // [non-loss-asset-filter]
    if (availableReserveAssets.isEmpty) {
      if (log.isTraceEnabled) log.trace(s"Currently no reserve asset available with a good exchange-rate to convert back $coins")
      None
    }

    val bestAvailableReserveAssets: List[Asset] =
      availableReserveAssets
        .sortBy(e => e._2) // sorted by rating
        .map(_._1) // asset only
        .reverse // best rating first

    val reserveAssetsNeedFillUp: Set[Asset] = reserveAssetsWhichNeedFillUp
    var destinationReserveAsset: Option[Asset] = bestAvailableReserveAssets.find(reserveAssetsNeedFillUp.contains) // [fill-up]
    if (destinationReserveAsset.isDefined && log.isTraceEnabled)
      log.trace(s"transferring $coins back to reserve asset ${destinationReserveAsset.get} [fill-up]")

    if (destinationReserveAsset.isEmpty) {
      destinationReserveAsset = Some(availableReserveAssets.head._1) // [play safe]
      if (log.isTraceEnabled) log.trace(s"transferring $coins back to reserve asset ${destinationReserveAsset.get} [play safe]")
    }

    val tradePair = TradePair.of(coins.asset, destinationReserveAsset.get)
    val limit: Double = guessGoodLimit(tradePair, TradeSide.Sell, coins.amount)
    val orderRequest = OrderRequest(
      UUID.randomUUID(),
      null,
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

  def convertBackNotNeededNoneReserveAssetLiquidity(): List[CryptoValue] = {
    val free: Map[Asset, Double] = determineFreeBalance
    val freeUnusedNoneReserveAssetLiquidity: Map[Asset, Double] = {
      free.map(e => (e._1,
        Math.max(0.0,
          e._2 - liquidityDemand.values.find(_.asset == e._1).map(_.amount).getOrElse(0.0))
      ))
    }

    var incomingLiquidity: List[CryptoValue] = List()
    for (f <- freeUnusedNoneReserveAssetLiquidity) {
      incomingLiquidity = convertBackToReserveAsset(CryptoValue(f._1, f._2)).toList ::: incomingLiquidity
    }
    incomingLiquidity
  }

  def rebalanceReserveAssetsAmountOrders(incomingReserveLiquidity: List[CryptoValue]): List[OrderRequest] = {
    val reserveAssetStack: List[CryptoValue] =
      incomingReserveLiquidity ::: // incomingReserveLiquidity can contain more than one entry per asset
        wallet.balance
          .filter(e => config.reserveAssets.contains(e._1))
          .map(e => CryptoValue(e._1, e._2.amountAvailable))
          .toList

    val reserveAssetsAggregated: Iterable[CryptoValue] =
      reserveAssetStack
        .groupBy(_.asset)
        .map(e => CryptoValue(e._1, e._2.map(_.amount).sum))

    // a bucket is a portion of an reserve asset having a specific value (value is configured in 'rebalance-tx-granularity-in-usdt')


    // all reserve assets, that
    // Map(Asset -> liquidity buckets missing)
    var liquiditySinkBuckets: Map[Asset, Int] =
      reserveAssetsAggregated
        .map(e => (e.asset, e.convertTo(USDT, referenceTicker).get.amount)) // amount in USDT
        .filter(_._2 < config.minimumKeepReserveLiquidityPerAssetInUSDT) // below min keep amount?
        .map(e => (e._1, ((config.minimumKeepReserveLiquidityPerAssetInUSDT - e._2) / config.rebalanceTxGranularityInUSDT).ceil.toInt)) // buckets needed
        .toMap
    // List(Asset + liquidity buckets available for distribution)
    var liquiditySourcesBuckets: Map[Asset, Int] =
      reserveAssetsAggregated
        .map(e => (e.asset, e.convertTo(USDT, referenceTicker).get.amount)) // amount in USDT
        .filter(e => e._2 > config.minimumKeepReserveLiquidityPerAssetInUSDT + config.rebalanceTxGranularityInUSDT) // having more than the minimum limit + tx-granularity
        .map(e => (e._1, ((e._2 - config.minimumKeepReserveLiquidityPerAssetInUSDT) / config.rebalanceTxGranularityInUSDT).floor.toInt)) // buckets to provide
        .toMap

    def removeBuckets(liquidityBuckets: Map[Asset, Int], asset:Asset, buckets:Int): Map[Asset, Int] = {
      liquidityBuckets.map {
        case (k, v) if k == asset => (k, v - buckets)
        case (k, v) => (k, v)
      }.filterNot(_._2 == 0)
    }

    log.debug(s"rebalance reserve assets: sources: $liquiditySourcesBuckets / sinks: $liquiditySinkBuckets")

    // try to satisfy all liquiditySinks using available sources
    // if multiple sources are available, we take from least priority reserve assets first
    var liquidityTx: List[OrderRequest] = Nil
    while (liquiditySinkBuckets.nonEmpty && liquiditySourcesBuckets.nonEmpty) {
      val sinkAsset = config.reserveAssets.find(liquiditySinkBuckets.keySet.contains).get
      val sourceAsset = config.reserveAssets.reverse.find(liquiditySourcesBuckets.keySet.contains).get
      val bucketsToTransfer = Math.min(liquiditySinkBuckets(sinkAsset), liquiditySourcesBuckets(sourceAsset))
      val (tradePair, tradeSide) = TradePair.of(sinkAsset, sourceAsset) match {
        case tp if tpData.ticker.contains(tp) => (tp, TradeSide.Buy)
        case tp if tpData.ticker.contains(tp.reverse) => (tp.reverse, TradeSide.Sell)
        case _ => throw new RuntimeException(s"No tradepair found to convert $sourceAsset to $sinkAsset")
      }
      val baseAssetBucketAmount: Double = CryptoValue(USDT, config.rebalanceTxGranularityInUSDT).convertTo(tradePair.baseAsset, tpData.ticker) match {
        case Some(value) => value.amount
        case None => throw new RuntimeException(s"Unable to convert ${tradePair.baseAsset} to USDT")
      }
      val orderAmountBaseAsset = bucketsToTransfer * baseAssetBucketAmount
      liquidityTx = OrderRequest(
        UUID.randomUUID(),
        null,
        exchangeConfig.exchangeName,
        tradePair,
        tradeSide,
        exchangeConfig.fee,
        orderAmountBaseAsset,
        guessGoodLimit(tradePair, tradeSide, orderAmountBaseAsset)
      ) :: liquidityTx

      liquiditySinkBuckets = removeBuckets(liquiditySinkBuckets, sinkAsset, bucketsToTransfer)
      liquiditySourcesBuckets = removeBuckets(liquiditySourcesBuckets, sourceAsset, bucketsToTransfer)
    }
    log.debug(s"Rebalance orders: $liquidityTx. Remaining sinks: $liquiditySinkBuckets")
    liquidityTx
  }

  // Management takes care, that we follow the liquidity providing & back-converting strategy (described above)
  def houseKeeping(): Unit = {
    clearObsoleteLocks()
    clearObsoleteDemands()
    val demanded: List[CryptoValue] = provideDemandedLiquidity()
    val incomingReserveLiquidity: List[CryptoValue] = convertBackNotNeededNoneReserveAssetLiquidity()

    val totalIncomingReserveLiquidity: List[CryptoValue] =
      incomingReserveLiquidity ::: demanded.filter(e => config.reserveAssets.contains(e.asset))
    for (o: OrderRequest <- rebalanceReserveAssetsAmountOrders(totalIncomingReserveLiquidity)) {
      tradeRoom ! LiquidityTransformationOrder(o)
    }
  }

  override def receive: Receive = {
    case r: LiquidityRequest =>
      checkValidity(r)
      sender() ! lockLiquidity(r)

    case LiquidityLockClearance(id) =>
      clearLock(id)

    case d: LiquidityDemand =>
      noticeDemand(d)
      houseKeeping()

    case _: WalletUpdateTrigger =>
      houseKeeping()
  }
}

// TODO statistics: min/max/average time a liquidity providing order needs to be filled
// TODO statistics: min/max/average time a convert back to reserve-liquidity tx needs to be filled
// TODO statistics: total number and final balance of all done liquidity providing and back-converting transactions
