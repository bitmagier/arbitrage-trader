package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.{NoSuchElementException, UUID}

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.{Balance, ExchangePublicDataReadonly, Ticker}
import org.purevalue.arbitrage.traderoom.TradeRoom.{FinishedLiquidityTx, LiquidityTransformationOrder, LiquidityTx, OrderRef}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.{Finished, RunWithWorkingContext}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, OrderBookTooFlatException, UniqueDemand}
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.{Config, ExchangeConfig, LiquidityManagerConfig}
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object LiquidityBalancer {
  case class RunWithWorkingContext(balanceSnapshot: Map[Asset, Balance],
                                   liquidityDemand: Map[String, UniqueDemand],
                                   liquidityLocks: Map[UUID, LiquidityLock])
  case class Finished()

  def props(config: Config,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            publicData: ExchangePublicDataReadonly,
            findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
            findFinishedLiquidityTx: (FinishedLiquidityTx => Boolean) => Option[FinishedLiquidityTx],
            referenceTicker: () => collection.Map[TradePair, Ticker],
            tradeRoom: ActorRef
           ): Props = Props(new LiquidityBalancer(config, exchangeConfig, tradePairs, publicData, findOpenLiquidityTx, findFinishedLiquidityTx, referenceTicker, tradeRoom))
}
class LiquidityBalancer(val config: Config,
                        val exchangeConfig: ExchangeConfig,
                        val tradePairs: Set[TradePair],
                        val publicData: ExchangePublicDataReadonly,
                        val findOpenLiquidityTx: (LiquidityTx => Boolean) => Option[LiquidityTx],
                        val findFinishedLiquidityTx: (FinishedLiquidityTx => Boolean) => Option[FinishedLiquidityTx],
                        val referenceTicker: () => collection.Map[TradePair, Ticker],
                        val tradeRoom: ActorRef
                       ) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[LiquidityManager])
  val c: LiquidityManagerConfig = config.liquidityManager

  def determineUnlockedBalance(wc: RunWithWorkingContext): Map[Asset, Double] =
    LiquidityManager.determineUnlockedBalance(wc.balanceSnapshot, wc.liquidityLocks, exchangeConfig)


  def determineRealisticLimit(tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double): Double = {
    LiquidityManager.determineRealisticLimit(publicData.orderBook, publicData.ticker, tradePair, tradeSide, amountBaseAsset, c.txLimitAwayFromEdgeLimit)
  }

  def localExchangeRateRating(tradePair: TradePair, tradeSide: TradeSide, amountBaseAsset: Double): Double = {
    LiquidityManager.localExchangeRateRating(publicData.orderBook, publicData.ticker, referenceTicker(), tradePair, tradeSide, amountBaseAsset, c.txLimitAwayFromEdgeLimit)
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
   * Try to find the optimal order to place, for satisfying the demanded value from a reserve asset.
   * If that one was found and placed, the expected incoming value is returned, otherwise None is returned
   */
  def tryToFindAGoodLiquidityProvidingOrder(demand: UniqueDemand, wc: RunWithWorkingContext): Option[LiquidityTransformationOrder] = {
    val bestReserveAssets: Option[Tuple2[Asset, Double]] =
      exchangeConfig.reserveAssets
        .filterNot(demand.dontUseTheseReserveAssets.contains)
        .filter(e => tradePairs.contains(TradePair(demand.asset, e)))
        .filter(e => estimatedManufactingCosts(demand, e) < wc.balanceSnapshot.get(e).map(_.amountAvailable).getOrElse(0.0)) // enough balance available?
        .map(e => (e, localExchangeRateRating(TradePair(demand.asset, e), TradeSide.Buy, demand.amount))) // join local exchange rate rating
        .filter(_._2 >= -c.maxAcceptableExchangeRateLossVersusReferenceTicker) // local rate acceptable?
        .sortBy(e => e._2)
        .lastOption

    if (bestReserveAssets.isEmpty) {
      log.debug(s"No good reserve asset found to satisfy $demand")
      None
    } else {
      if (log.isTraceEnabled) log.trace(s"Found best usable reserve asset: ${bestReserveAssets.get._1}, rating=${bestReserveAssets.get._2} for providing $demand")
      val orderAmount: Double = demand.amount * (1.0 + c.providingLiquidityExtra)
      val tradePair = TradePair(demand.asset, bestReserveAssets.get._1)
      val limit = determineRealisticLimit(tradePair, TradeSide.Buy, orderAmount)
      val orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, tradePair, TradeSide.Buy, exchangeConfig.feeRate, orderAmount, limit)

      log.info(s"${Emoji.LookingDown}  [${exchangeConfig.name}] Unable to provide liquidity demand: $demand")
      Some(LiquidityTransformationOrder(orderRequest))
    }
  }


  // aim: free available liquidity shall be available to fulfill the sum of demand
  def provideDemandedLiquidity(wc: RunWithWorkingContext): Seq[UniqueDemand] = {
    val unlocked: Map[Asset, Double] = determineUnlockedBalance(wc)
    // unsatisfied demand:
    wc.liquidityDemand.values
      .map(e =>
        UniqueDemand(
          e.tradePattern,
          e.asset,
          Math.max(0.0, e.amount - unlocked.getOrElse(e.asset, 0.0)),
          e.dontUseTheseReserveAssets,
          e.lastRequested)) // missing coins
      .filter(_.amount != 0.0)
      .toSeq
  }

  def liquidityConversionPossibleBetween(a: Asset, b: Asset): Boolean = {
    tradePairs.contains(TradePair(a, b)) || tradePairs.contains(TradePair(b, a))
  }

  def rebalanceReserveAssets(pendingIncomingReserveLiquidity: Seq[CryptoValue],
                             wc: RunWithWorkingContext): Seq[LiquidityTransformationOrder] = {
    if (log.isTraceEnabled) log.trace(s"re-balancing reserve asset wallet:${wc.balanceSnapshot} with pending incoming $pendingIncomingReserveLiquidity")
    val currentReserveAssetsBalance: Seq[CryptoValue] = wc.balanceSnapshot
      .filter(e => exchangeConfig.reserveAssets.contains(e._1))
      .map(e => CryptoValue(e._1, e._2.amountAvailable))
      .toSeq ++
      exchangeConfig.reserveAssets // add zero balance for reserve assets not contained in wallet
        .filterNot(wc.balanceSnapshot.keySet.contains)
        .map(e => CryptoValue(e, 0.0)) // so consider not delivered, empty balances too

    val virtualReserveAssetsAggregated: Iterable[CryptoValue] =
      (pendingIncomingReserveLiquidity ++ currentReserveAssetsBalance)
        .groupBy(_.asset)
        .map(e => CryptoValue(e._1, e._2.map(_.amount).sum))

    val unableToConvert: Iterable[CryptoValue] = virtualReserveAssetsAggregated.filter(e => !e.canConvertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))
    if (unableToConvert.nonEmpty) {
      log.warn(s"${Emoji.EyeRoll}  [${exchangeConfig.name}] Some reserve assets cannot be judged, because we cannot convert them to USD, so no liquidity transformation is possible for them: $unableToConvert")
    }

    // a bucket is a portion of an reserve asset having a specific value (value is configured in 'rebalance-tx-granularity-in-usd')

    // all reserve assets, that need more value : Map(Asset -> liquidity buckets missing)
    var liquiditySinkBuckets: Map[Asset, Int] = virtualReserveAssetsAggregated
      .filter(_.canConvertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))
      .map(e => (e.asset, e.convertTo(exchangeConfig.usdEquivalentCoin, referenceTicker()).amount)) // amount in USD
      .filter(_._2 < c.minimumKeepReserveLiquidityPerAssetInUSD) // below min keep amount?
      .map(e => (e._1, ((c.minimumKeepReserveLiquidityPerAssetInUSD - e._2) / c.rebalanceTxGranularityInUSD).ceil.toInt)) // buckets needed
      .toMap
    // all reserve assets, that have extra liquidity to distribute : Map(Asset -> liquidity buckets available for distribution)
    var liquiditySourcesBuckets: Map[Asset, Int] = currentReserveAssetsBalance // we can take coin only from currently really existing balance
      .filter(_.canConvertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))
      .map(e => (e.asset, e.convertTo(exchangeConfig.usdEquivalentCoin, referenceTicker()).amount)) // amount in USD
      // having more than the minimum limit + 150% tx-granularity (to avoid useless there-and-back transfers because of exchange rate fluctuations)
      .filter(_._2 >= c.minimumKeepReserveLiquidityPerAssetInUSD + c.rebalanceTxGranularityInUSD * 1.5)
      // keep minimum reserve liquidity + 50% bucket value (we don't sell our last half-full extra bucket ^^^)
      .map(e => (e._1, ((e._2 - c.minimumKeepReserveLiquidityPerAssetInUSD - c.rebalanceTxGranularityInUSD * 0.5) / c.rebalanceTxGranularityInUSD).floor.toInt)) // buckets to provide
      .toMap

    def removeBuckets(liquidityBuckets: Map[Asset, Int], asset: Asset, numBuckets: Int): Map[Asset, Int] = {
      // asset or numBuckets may not exist
      liquidityBuckets.map {
        case (k, v) if k == asset => (k, Math.max(0, v - numBuckets)) // don't go below zero when existing number of buckets is smaller that numBuckets to remove (may happen for DefaultSink)
        case (k, v) => (k, v)
      }.filterNot(_._2 == 0)
    }

    var liquidityTxOrders: List[OrderRequest] = Nil

    // try to satisfy all liquiditySinks using available sources
    // if multiple sources are available, we take from least priority reserve assets first

    var weAreDoneHere: Boolean = false
    while (liquiditySourcesBuckets.nonEmpty && !weAreDoneHere) {
      if (log.isTraceEnabled) log.trace(s"Re-balance reserve assets: sources: $liquiditySourcesBuckets / sinks: $liquiditySinkBuckets")
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

            case tp if publicData.ticker.contains(tp) =>
              val tradePair = tp
              val tradeSide = TradeSide.Buy
              val baseAssetBucketValue: Double = CryptoValue(exchangeConfig.usdEquivalentCoin, c.rebalanceTxGranularityInUSD)
                .convertTo(tradePair.baseAsset, publicData.ticker).amount

              val orderAmountBaseAsset = bucketsToTransfer * baseAssetBucketValue
              val limit = determineRealisticLimit(tradePair, tradeSide, orderAmountBaseAsset)
              OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, tradePair, tradeSide, exchangeConfig.feeRate, orderAmountBaseAsset, limit)

            case tp if publicData.ticker.contains(tp.reverse) =>
              val tradePair = tp.reverse
              val tradeSide = TradeSide.Sell
              val quoteAssetBucketValue: Double = CryptoValue(exchangeConfig.usdEquivalentCoin, c.rebalanceTxGranularityInUSD)
                .convertTo(tradePair.quoteAsset, publicData.ticker).amount

              val amountBaseAssetEstimate = bucketsToTransfer * quoteAssetBucketValue / publicData.ticker(tradePair).priceEstimate
              val limit = determineRealisticLimit(tradePair, tradeSide, amountBaseAssetEstimate)
              val orderAmountBaseAsset = bucketsToTransfer * quoteAssetBucketValue / limit
              OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, tradePair, tradeSide, exchangeConfig.feeRate, orderAmountBaseAsset, limit)

            case _ => throw new RuntimeException(s"${exchangeConfig.name}: No local tradepair found to convert $sourceAsset to $sinkAsset")
          }

        liquidityTxOrders = tx :: liquidityTxOrders

        liquiditySinkBuckets = removeBuckets(liquiditySinkBuckets, sinkAsset, bucketsToTransfer)
        liquiditySourcesBuckets = removeBuckets(liquiditySourcesBuckets, sourceAsset.get, bucketsToTransfer)
      }
    }

    // merge possible split orders towards primary reserve asset
    liquidityTxOrders = liquidityTxOrders
      .groupBy(e => (e.tradePair, e.tradeSide))
      .map { e =>
        val f = e._2.head
        val amount = e._2.map(_.amountBaseAsset).sum
        OrderRequest(f.id, f.orderBundleId, f.exchange, f.tradePair, f.tradeSide, f.feeRate, amount, f.limit)
      }.toList

    liquidityTxOrders.map(LiquidityTransformationOrder)
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

  def reserveAssetsWithLowLiquidity(wc: RunWithWorkingContext): Set[Asset] = {
    try {
      val fromWallet: Set[Asset] =
        wc.balanceSnapshot
          .filter(e => exchangeConfig.reserveAssets.contains(e._1))
          .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
          .filterNot(_._1.isFiat)
          .filter(_._1.canConvertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))
          .map(e => (e, CryptoValue(e._1, e._2.amountAvailable).convertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))) // + USD value - we expect we can convert a reserve asset to USD-equivalent-coin always
          .filter(e => e._2.amount < c.minimumKeepReserveLiquidityPerAssetInUSD) // value below minimum asset liquidity value
          .map(_._1._1)
          .toSet

      val remainingReserveAssets = exchangeConfig.reserveAssets.filter(fromWallet.contains)
      fromWallet ++ remainingReserveAssets
    } catch {
      case e: NoSuchElementException =>
        throw new RuntimeException(s"Sorry, can't work with reserve assets in ${exchangeConfig.name} wallet, which cannot be converted to USD. This is the wallet: ${wc.balanceSnapshot}")
    }
  }

  //  [Concept Reserve-Liquidity-Management] (copied from description above)
  //  - Unused (not locked or demanded) liquidity of a non-Reserve-Asset will be automatically converted to a Reserve-Asset.
  //    Which reserve-asset it will be, is determined by:
  //    - [non-loss-asset-filter] Filtering acceptable exchange-rates based on ticker on that exchange compared to a [ReferenceTicker]-value
  //    - [fill-up] Try to reach minimum configured balance of each reserve-assets in their order of preference
  //    - [play safe] Remaining value goes to first (highest prio) reserve-asset (having a acceptable exchange-rate)
  def convertBackToReserveAsset(cryptoValue: CryptoValue, wc: RunWithWorkingContext): Option[LiquidityTransformationOrder] = {
    val possibleReserveAssets: List[Tuple2[Asset, Double]] =
      exchangeConfig.reserveAssets
        .filter(e => publicData.ticker.contains(TradePair(cryptoValue.asset, e))) // filter available TradePairs only (with a local ticker)
        .map(e => (e, localExchangeRateRating(TradePair(cryptoValue.asset, e), TradeSide.Sell, cryptoValue.amount))) // add local exchange rate rating

    val availableReserveAssets: List[Tuple2[Asset, Double]] =
      possibleReserveAssets
        .filter(_._2 >= -c.maxAcceptableExchangeRateLossVersusReferenceTicker) // [non-loss-asset-filter]

    if (availableReserveAssets.isEmpty) {
      if (possibleReserveAssets.isEmpty) log.debug(s"No reserve asset available to convert back $cryptoValue")
      else log.debug(s"Currently no reserve asset available with a good exchange-rate to convert back $cryptoValue. " +
        s"Available assets/ticker-rating: $availableReserveAssets")
      return None
    }

    val bestAvailableReserveAssets: List[Asset] =
      availableReserveAssets
        .sortBy(e => e._2) // sorted by local exchange rate rating
        .map(_._1) // asset only
        .reverse // best rating first

    val reserveAssetsNeedFillUp: Set[Asset] = reserveAssetsWithLowLiquidity(wc)
    var destinationReserveAsset: Option[Asset] = bestAvailableReserveAssets.find(reserveAssetsNeedFillUp.contains) // [fill-up]
    if (destinationReserveAsset.isDefined) {
      log.info(s"${Emoji.ThreeBitcoin}  [${exchangeConfig.name}] transferring $cryptoValue back to reserve asset ${destinationReserveAsset.get} [fill-up]")
    }

    if (destinationReserveAsset.isEmpty) {
      destinationReserveAsset = Some(availableReserveAssets.head._1) // [play safe]
      log.info(s"${Emoji.ThreeBitcoin}  [${exchangeConfig.name}] transferring $cryptoValue back to reserve asset ${destinationReserveAsset.get} [primary sink]")
    }

    val tradePair = TradePair(cryptoValue.asset, destinationReserveAsset.get)
    val limit: Double = determineRealisticLimit(tradePair, TradeSide.Sell, cryptoValue.amount)
    val orderRequest = OrderRequest(
      UUID.randomUUID(),
      None,
      exchangeConfig.name,
      tradePair,
      TradeSide.Sell,
      exchangeConfig.feeRate,
      cryptoValue.amount,
      limit
    )
    Some(LiquidityTransformationOrder(orderRequest))
  }

  def convertBackNotNeededNoneReserveAssetLiquidity(wc: RunWithWorkingContext): Seq[LiquidityTransformationOrder] = {
    val freeUnusedNoneReserveAssetLiquidity: Map[Asset, Double] =
      determineUnlockedBalance(wc)
        .filterNot(e => exchangeConfig.reserveAssets.contains(e._1)) // only select non-reserve-assets
        .filterNot(e => convertBackLiquidityTxActive(e._1)) // don't issue another liquidity tx for an asset, where another tx one is still active
        .map(e => ( // reduced by demand for that asset
          e._1,
          Math.max(
            0.0,
            e._2 - wc.liquidityDemand.values
              .filter(_.asset == e._1)
              .map(_.amount)
              .sum
          )))
        .filter(_._2 > 0.0) // remove empty values
        .filter(_._1.canConvertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))
        .map(e => (e._1, e._2, CryptoValue(e._1, e._2).convertTo(exchangeConfig.usdEquivalentCoin, publicData.ticker))) // join USDT value
        .filter(e => e._3.amount >= c.dustLevelInUSD) // ignore values below the dust level
        .map(e => (e._1, e._2))
        .toMap

    freeUnusedNoneReserveAssetLiquidity.flatMap { f =>
      convertBackToReserveAsset(CryptoValue(f._1, f._2), wc)
    }.toSeq
  }


  def unfinishedOrders(orderRefs: Seq[OrderRef]): Seq[OrderRef] = {
    orderRefs.filterNot(e => findFinishedLiquidityTx(_.finishedOrder.ref == e).isDefined)
  }

  def balanceLiquidity(wc: RunWithWorkingContext): Unit = {
    val orderFinishDeadline = Instant.now.plus(config.tradeRoom.maxOrderLifetime.multipliedBy(2))
    try {
      val unsatisfiedDemand: Seq[UniqueDemand] = provideDemandedLiquidity(wc)
      val unsatisfiedDemandOrders: Seq[LiquidityTransformationOrder] = unsatisfiedDemand.flatMap { d =>
        tryToFindAGoodLiquidityProvidingOrder(d, wc)
      }

      val incomingReserveLiquidityOrders: Seq[LiquidityTransformationOrder] = convertBackNotNeededNoneReserveAssetLiquidity(wc)

      val totalIncomingReserveLiquidity = (incomingReserveLiquidityOrders ++ unsatisfiedDemandOrders)
        .map(_.orderRequest.calcIncomingLiquidity)
        .map(_.cryptoValue)
        .filter(e => exchangeConfig.reserveAssets.contains(e.asset))

      val rebalanceReserveAssetsOrders = rebalanceReserveAssets(totalIncomingReserveLiquidity, wc)

      implicit val timeout: Timeout = config.global.internalCommunicationTimeout
      var futureOrderRefs: List[Future[Option[OrderRef]]] = Nil
      try {
        for (liquidityTxOrder <- unsatisfiedDemandOrders ++ incomingReserveLiquidityOrders ++ rebalanceReserveAssetsOrders) {
          futureOrderRefs = (tradeRoom ? liquidityTxOrder).mapTo[Option[OrderRef]] :: futureOrderRefs
        }
      } catch {
        case e: Exception => log.error("Error while placing liquidity tx orders", e)
      }

      val orderRefs: Seq[OrderRef] = Await.result(Future.sequence(futureOrderRefs), timeout.duration).flatten
      while (unfinishedOrders(orderRefs).nonEmpty && Instant.now.isBefore(orderFinishDeadline)) {
        Thread.sleep(200)
      }
      val stillUnfinished = unfinishedOrders(orderRefs)
      if (stillUnfinished.nonEmpty) {
        throw new RuntimeException(s"Not all liquidity tx orders did finish. Still unfinished: [$stillUnfinished]") // should not happen, because TradeRoom/Exchange cleanup unfinsihed orders by themself!
      } else {
        log.debug(s"all ${orderRefs.size} liquidity transaction(s) finished")
        sender() ! Finished()
      }
    } catch {
      case e: OrderBookTooFlatException =>
        log.warn(s"[to be improved] [${exchangeConfig.name}] Cannot perform liquidity housekeeping because the order book of tradepair ${e.tradePair} was too flat")
    }
  }

  override def receive: Receive = {
    case wc: RunWithWorkingContext => balanceLiquidity(wc)
  }
}
