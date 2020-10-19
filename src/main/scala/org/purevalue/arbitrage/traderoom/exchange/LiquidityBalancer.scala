package org.purevalue.arbitrage.traderoom.exchange

import java.util.UUID

import akka.event.Logging
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.traderoom.TradeRoom.NewLiquidityTransformationOrder
import org.purevalue.arbitrage.traderoom.TradeSide.{Buy, Sell}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.{LiquidityTransfer, WorkingContext}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, UniqueDemand}
import org.purevalue.arbitrage.traderoom.{Asset, CryptoValue, OrderRequest, TradePair, TradeSide}
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{Config, ExchangeConfig}

import scala.annotation.tailrec

trait LBStatisticEvent {
  def exchange: String
}
case class NoGoodManufacturingOptionAvailable(exchange: String, demandAsset: Asset) extends LBStatisticEvent
case class LiquidityRebalanceCurrentlyLossy(exchange: String, source: Asset, destination: Asset) extends LBStatisticEvent

object LiquidityBalancerStats {
  private val log = Logging(actorSystem.eventStream, getClass)
  private var stats: Map[LBStatisticEvent, Int] = Map()

  def inc(e: LBStatisticEvent): Unit = {
    stats.synchronized {
      stats = stats.updated(e, stats.getOrElse(e, 0) + 1)
    }
  }

  def logStats(): Unit = {
    log.info(
      s"""LiquidityBalancer statistic:
         |${stats.mkString("\n")}""".stripMargin)
  }
}

object LiquidityBalancer {
  case class WorkingContext(ticker: Map[TradePair, Ticker],
                            referenceTicker: Map[TradePair, Ticker],
                            orderBook: Map[TradePair, OrderBook],
                            balanceSnapshot: Map[Asset, Balance],
                            liquidityDemand: Map[String, UniqueDemand],
                            liquidityLocks: Map[UUID, LiquidityLock])

  case class LiquidityTransfer(pair: TradePair, side: TradeSide, buckets: Int, quantity: Double, limit: Double, exchangeRateRating: Double) {
    def supplyAsset: Asset = if (side == Buy) pair.quoteAsset else pair.baseAsset

    def sinkAsset: Asset = if (side == Buy) pair.baseAsset else pair.quoteAsset
  }
}
class LiquidityBalancer(val config: Config,
                        val exchangeConfig: ExchangeConfig,
                        val tradePairs: Set[TradePair],
                        val wc: WorkingContext) {
  private val log = Logging(actorSystem.eventStream, getClass)

  // tx granularity = asset's bucket size
  def bucketSize(asset: Asset): Double =
    CryptoValue(exchangeConfig.usdEquivalentCoin, config.liquidityManager.txValueGranularityInUSD)
      .convertTo(asset, wc.ticker)
      .amount

  def toSupplyBuckets(asset: Asset,
                      amount: Double): Int =
    Math.max(0, (amount / bucketSize(asset)).round.toInt - 1)

  def toBucketsRound(asset: Asset,
                     amount: Double): Int =
    (amount / bucketSize(asset)).round.toInt

  def createOrders(transfers: Iterable[LiquidityTransfer]): Iterable[NewLiquidityTransformationOrder] = {
    transfers.map(e => NewLiquidityTransformationOrder(
      OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, e.pair, e.side, exchangeConfig.feeRate, e.quantity, e.limit))
    )
  }

  // (demand - supply) & greater than zero
  // demand-buckets = (amount / tx-granularity-of-asset).round + 1
  def calcUnsatisfiedDemand: Map[Asset, Int] = {
    val pureDemand: Map[Asset, Double] = wc.liquidityDemand.values
      .map(e => (e.asset, e.amount))
      .groupBy(_._1)
      .map(e => e._1 -> e._2.map(_._2).sum)

    pureDemand.map {
      case (asset, pureDemand) =>
        val resultingDemand: Double = pureDemand - wc.balanceSnapshot.get(asset).map(_.amountAvailable).getOrElse(0.0)
        val demandBuckets: Int = if (resultingDemand <= 0.0) 0 else (resultingDemand / bucketSize(asset)).round.toInt + 1
        (asset, demandBuckets)
    }
  }

  // (supply - max(demand, lock)) & greater-than-zero
  // supply-buckets = (amount / tx-granularity-of-asset).round - 1
  def calcPureSupplyOverhead: Map[Asset, Int] = {
    wc.balanceSnapshot
      .filterNot(_._1.isFiat)
      .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
      .map {
        case (asset, balance) =>
          val demand: Double = wc.liquidityDemand.values.find(_.asset == asset).map(_.amount).getOrElse(0.0)
          val locked: Double = wc.liquidityLocks.values.flatMap(_.coins).filter(_.asset == asset).map(_.amount).sum
          val overheadAmount: Double = balance.amountAvailable - Math.max(demand, locked)
          val overheadBuckets: Int = toSupplyBuckets(asset, overheadAmount)
          (asset, overheadBuckets)
      }.filter(_._2 > 0.0)
  }

  def tradePairAndSide(source: Asset, destination: Asset): (TradePair, TradeSide) = {
    // @formatter:off
    tradePairs.find(e => e.baseAsset == destination && e.quoteAsset == source) match {
      case Some(pair) => (pair, Buy)
      case None       => (TradePair(source, destination), Sell)
    } // @formatter:on
  }

  /**
   * Determines remaining supply = supply minus all transfers from that supply.
   * Will throw an IllegalArgumentException if one transfer adds a bucket to supply
   */
  def calcRemainingSupply(supply: Map[Asset, Int],
                          transfers: Iterable[LiquidityTransfer]): Map[Asset, Int] = {
    var result = supply
    for (t <- transfers) {
      if (supply.contains(t.sinkAsset)) throw new IllegalArgumentException(s"supply $supply contains transfer target asset ${t.sinkAsset.officialSymbol}. Transfers: $transfers")
      if (!supply.contains(t.supplyAsset)) throw new IllegalArgumentException(s"transfer $t not covered by supply: $supply")

      result = result(t.supplyAsset) - t.buckets match {
        case tooLow: Int if tooLow < 0 => throw new IllegalArgumentException(s"found negative supply left")
        case zero: Int if zero == 0 => result - t.supplyAsset
        case remainingBuckets: Int => result.updated(t.supplyAsset, remainingBuckets)
      }
    }
    result
  }

  /**
   * Fill unsatisfied-demands (in order) using supply-overhead:
   * <pre>
   * [1] when there is a trade pair available
   * [2] and when exchange-rate is acceptable
   * [3] then use one with the best exchange rate rating
   * [4] if multiple options are left, use non-reserve-assets first (any), then use reserve-assets (in reverse order)
   * </pre>
   *
   * @return total number of (possible) transactions (in buckets) to satisfy the demand
   */
  def fillDemand(purpose:String,
                 unsatisfiedDemand: List[(Asset, Int)],
                 supplyOverhead: Map[Asset, Int]): List[LiquidityTransfer] = {
    /**
     * @return a number indicating the asset's preference level regarding option [4]
     *         a higher preference-level is preferred over a lower one
     */
    def sourceAssetPreference(asset: Asset): Int = {
      if (exchangeConfig.reserveAssets.contains(asset)) {
        exchangeConfig.reserveAssets.indexOf(asset) // position of reserve asset
      } else {
        exchangeConfig.reserveAssets.size // one number above reserve assets for any non-reserve asset
      }
    }

    /**
     * @return <pre>
     * (remainingDemandBuckets:Int,
     *  remainingSupply:Map[Asset,Int],
     *  transfer:Option[LiquidityBucketTransfer])</pre>
     */
    def findTransfer(demandAsset: Asset,
                     demandBuckets: Int,
                     supply: Map[Asset, Int]):
    (Int, Map[Asset, Int], Option[LiquidityTransfer]) = {

      if (demandBuckets == 0) return (demandBuckets, supply, None)

      val transferOptions =
        supply
          .filter(_._1.canConvertDirectlyTo(demandAsset, tradePairs)) // transfer possible
          .map(e => {
            val (pair, side) = tradePairAndSide(e._1, demandAsset)
            val buckets: Int = Math.min(demandBuckets, e._2)
            val quantity: Double = buckets * bucketSize(pair.baseAsset)
            val limit = determineRealisticLimit(pair, side, quantity, config.liquidityManager.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
            val exchangeRateRating: Double = localExchangeRateRating(pair, side, limit, wc.referenceTicker)
            LiquidityTransfer(pair, side, buckets, quantity, limit, exchangeRateRating)
          })
          .filter(_.exchangeRateRating >= -config.liquidityManager.maxAcceptableExchangeRateLossVersusReferenceTicker) // rating above loss limit

      transferOptions match {
        case o if o.isEmpty =>
          LiquidityBalancerStats.inc(NoGoodManufacturingOptionAvailable(exchangeConfig.name, demandAsset))
          log.debug(s"[${exchangeConfig.name}] No good manufacturing option available for $demandAsset. Supply was: $supply")
          (demandBuckets, supply, None)

        case transferOptions =>
          val bestRating = transferOptions.map(_.exchangeRateRating).max
          val BestRatingEquivalenceLevel = 0.00001
          val transfer = transferOptions
            .filter(_.exchangeRateRating >= bestRating - BestRatingEquivalenceLevel)
            .maxBy(e => sourceAssetPreference(e.supplyAsset))
          (demandBuckets - transfer.buckets, calcRemainingSupply(supply, Seq(transfer)), Some(transfer))
      }
    }

    /**
     * @return (remainingDemand, remainingSupply, transfer)
     */
    @tailrec
    def findNextTransfer(demand: List[(Asset, Int)],
                         supply: Map[Asset, Int]):
    (List[(Asset, Int)], Map[Asset, Int], Option[LiquidityTransfer]) = {

      if (demand.isEmpty) (demand, supply, None)
      else
        findTransfer(demand.head._1, demand.head._2, supply) match {
          case (_, _, None) => findNextTransfer(demand.tail, supply)
          case (remainingDemandBuckets: Int, rs: Map[Asset, Int], Some(t: LiquidityTransfer)) if remainingDemandBuckets == 0 =>
            (demand.tail, rs, Some(t))
          case (remainingDemandBuckets: Int, rs: Map[Asset, Int], Some(t: LiquidityTransfer)) =>
            ((demand.head._1, remainingDemandBuckets) :: demand.tail, rs, Some(t))
        }
    }

    if (log.isDebugEnabled) log.debug(s"[${exchangeConfig.name}] [$purpose] fillDemand(unsatisfiedDemand:$unsatisfiedDemand, supplyOverhead:$supplyOverhead)")

    findNextTransfer(unsatisfiedDemand, supplyOverhead) match {
      case (_, _, None) => Nil
      case (remainingDemand, remainingSupply, Some(transfer)) => transfer :: fillDemand(purpose, remainingDemand, remainingSupply)
    }
  }

  /**
   * Remaining final supply overhead with respect to minKeepReserveBuckets
   */
  def calcFinalSupplyOverhead(supplyOverhead: Map[Asset, Int],
                              minimumKeepReserveAssetBuckets: Int): Map[Asset, Int] = {
    supplyOverhead
      .map { // @formatter:off
        case (asset, buckets) if exchangeConfig.reserveAssets.contains(asset) => (asset, Math.max(0, buckets - minimumKeepReserveAssetBuckets))
        case (asset, buckets)                                                 => (asset, buckets)
      } // @formatter:on
      .filter(_._2 != 0)
  }

  // = supply +- transfers
  def calcSupplyAfterTransfers(supply: Map[Asset, Int],
                               transfers: Iterable[LiquidityTransfer]): Map[Asset, Int] = {
    var result: Map[Asset, Int] = supply

    for (t <- transfers) {
      val diff: Seq[(Asset, Int)] = t.side match {
        case Buy => Seq((t.pair.baseAsset, t.buckets), (t.pair.quoteAsset, -t.buckets))
        case Sell => Seq((t.pair.quoteAsset, t.buckets), (t.pair.baseAsset, -t.buckets))
      }
      diff.foreach { d =>
        if (result.contains(d._1)) {
          result = result.updated(d._1, result(d._1) + d._2)
        }
      }
    }
    result
  }

  // delivers secondary reserve asset liquidity demand - sorted by importance of reserve asset
  def calcSecondaryReserveFillUpDemand(previousTransfers: Iterable[LiquidityTransfer],
                                       minimumKeepReserveAssetBuckets: Int): List[(Asset, Int)] = {
    val secondaryReserveBalanceBuckets: Map[Asset, Int] =
      exchangeConfig.reserveAssets.tail
        .map(e => e -> toSupplyBuckets(e, wc.balanceSnapshot.get(e).map(_.amountAvailable).getOrElse(0.0)))
        .toMap

    val secondaryReserveBalanceAfterTransfers: Map[Asset, Int] = calcSupplyAfterTransfers(secondaryReserveBalanceBuckets, previousTransfers)
    if (secondaryReserveBalanceAfterTransfers.exists(_._2 < 0)) throw new IllegalArgumentException(s"$secondaryReserveBalanceAfterTransfers")

    secondaryReserveBalanceAfterTransfers
      .map(e => e._1 -> (minimumKeepReserveAssetBuckets - e._2)) // asset -> demand
      .filter(_._2 > 0) // take only positive demands
      .toList
      .sortBy(e => exchangeConfig.reserveAssets.indexOf(e._1)) // sort by position of reserve asset
  }

  def transferAllSupplyTo(destination: Asset,
                          supply: Map[Asset, Int]): Iterable[LiquidityTransfer] = {
    supply
      .flatMap { e =>
        val (pair, side) = tradePairAndSide(e._1, destination)
        val quantity: Double = bucketSize(pair.baseAsset) * e._2
        val limit: Double = determineRealisticLimit(pair, side, quantity, config.liquidityManager.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
        val exchangeRateRating: Double = localExchangeRateRating(pair, side, limit, wc.referenceTicker)
        if (exchangeRateRating >= -config.liquidityManager.maxAcceptableExchangeRateLossVersusReferenceTicker) {
          Some(LiquidityTransfer(pair, side, e._2, quantity, limit, exchangeRateRating))
        } else {
          LiquidityBalancerStats.inc(LiquidityRebalanceCurrentlyLossy(exchangeConfig.name, e._1, destination))
          log.debug(s"[${exchangeConfig.name}] Liquidity rebalance currently lossy: ${e._1}->$destination, exchangeRateRating: ${formatDecimal(exchangeRateRating, 5)}}}")
          None
        }
      }
  }

  def squash(unsquashed: Iterable[LiquidityTransfer]): Iterable[LiquidityTransfer] = {
    unsquashed
      .groupBy(e => (e.supplyAsset, e.pair, e.side))
      .map { e =>
        val quantity = e._2.map(_.quantity).sum
        val limit = determineRealisticLimit(e._1._2, e._1._3, quantity, config.liquidityManager.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
        LiquidityTransfer(e._1._2, e._1._3, e._2.map(_.buckets).sum, quantity, limit, 0.0) // rating is not important any more, we just merge already approved single transfers and find an appropriate limit
      }
  }

  def calculateOrders(): Iterable[NewLiquidityTransformationOrder] = {
    try {
      // satisfy noticed liquidity demand
      val unsatisfiedDemand: List[(Asset, Int)] = calcUnsatisfiedDemand.toList.sortBy(_._2)
      val supplyOverhead: Map[Asset, Int] = calcPureSupplyOverhead
      val fillDemandTransfers: Iterable[LiquidityTransfer] = fillDemand("liquidity-demand", unsatisfiedDemand, supplyOverhead)

      // fill-up secondary reserve assets in order
      val minimumKeepReserveAssetBuckets: Int = toBucketsRound(exchangeConfig.usdEquivalentCoin, config.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD)
      val afterDemandsFilledSupplyOverhead: Map[Asset, Int] =
        calcFinalSupplyOverhead(
          calcRemainingSupply(supplyOverhead, fillDemandTransfers),
          minimumKeepReserveAssetBuckets
        ).filterNot(_._1 == exchangeConfig.primaryReserveAsset) // primary reserve asset is the default sink, so there is no supply overhead here anymore

      val secondaryReserveFillUpDemand: List[(Asset, Int)] = calcSecondaryReserveFillUpDemand(fillDemandTransfers, minimumKeepReserveAssetBuckets)
      val secondaryReserveFillUpTransfers: Iterable[LiquidityTransfer] = fillDemand("secondary reserve fill-up", secondaryReserveFillUpDemand, afterDemandsFilledSupplyOverhead)

      // transfer remaining supply overhead to primary reserve asset
      val primaryReserveInflowTransfers: Iterable[LiquidityTransfer] =
        transferAllSupplyTo(
          exchangeConfig.primaryReserveAsset,
          calcRemainingSupply(afterDemandsFilledSupplyOverhead, secondaryReserveFillUpTransfers))

      createOrders(
        squash(fillDemandTransfers ++ secondaryReserveFillUpTransfers ++ primaryReserveInflowTransfers)
      )
    } catch {
      case e:Exception =>
        log.error(s"[${exchangeConfig.name}] liquidity balancer failed.\nAvailable trade pairs: ${tradePairs.toSeq.sortBy(_.toString)}.", e)
        Nil
    }
  }
}
