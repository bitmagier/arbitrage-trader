package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.{NoSuchElementException, UUID}

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.traderoom.TradeRoom._
import org.purevalue.arbitrage.traderoom.TradeSide.{Buy, Sell}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.OldLiquidityBalancerRun.{Finished, Run, WorkingContext}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, OrderBookTooFlatException, UniqueDemand}
import org.purevalue.arbitrage.util.Emoji
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{Config, ExchangeConfig, LiquidityManagerConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success}

object OldLiquidityBalancerRun {
  case class WorkingContext(ticker: Map[TradePair, Ticker],
                            referenceTicker: Map[TradePair, Ticker],
                            orderBook: Map[TradePair, OrderBook],
                            balanceSnapshot: Map[Asset, Balance],
                            liquidityDemand: Map[String, UniqueDemand],
                            liquidityLocks: Map[UUID, LiquidityLock],
                            activeLiquidityTx: Map[OrderRef, LiquidityTx])
  private case class Run()
  case class Finished()

  def props(config: Config,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            parent: ActorRef,
            tradeRoom: ActorRef,
            wc: WorkingContext
           ): Props = Props(new OldLiquidityBalancerRun(config, exchangeConfig, tradePairs, parent, tradeRoom, wc))
}

// Temporary Actor - stops itself after successful run
class OldLiquidityBalancerRun(val config: Config,
                              val exchangeConfig: ExchangeConfig,
                              val tradePairs: Set[TradePair],
                              val parent: ActorRef,
                              val tradeRoom: ActorRef,
                              val wc: WorkingContext
                          ) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OldLiquidityBalancerRun])
  private val c: LiquidityManagerConfig = config.liquidityManager

  def determineUnlockedBalance(): Map[Asset, Double] =
    LiquidityManager.determineUnlockedBalance(wc.balanceSnapshot, wc.liquidityLocks, exchangeConfig)

  /**
   * Calculates manufacturing costs for the demand in the reserveAsset unit
   */
  def estimatedManufactingCosts(demand: UniqueDemand, reserveAsset: Asset): Double = {
    CryptoValue(demand.asset, demand.amount)
      .convertTo(reserveAsset, wc.ticker)
      .amount
  }

  def unfinishedOrders(orderRefs: Seq[OrderRef]): Future[Seq[OrderRef]] = {
//    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
//    val results: Seq[Future[Option[OrderRef]]] = orderRefs.map { r =>
//      (tradeRoom ? FindFinishedLiquidityTx(_.finishedOrder.ref == r)).mapTo[Option[FinishedLiquidityTx]]
//        .map(e => if (e.isDefined) None else Some(r))
//    }
//    Future.sequence(results).map(_.flatten)
    ???
  }

  def ceilToTxGranularity(asset: Asset, amount: Double): Double = {
    val granularity: Double = CryptoValue(exchangeConfig.usdEquivalentCoin, c.txValueGranularityInUSD)
      .convertTo(asset, wc.ticker)
      .amount

    (amount / granularity).ceil * granularity
  }


  def provideDemandedLiquidityOrders(): Seq[NewLiquidityTransformationOrder] = {
    /**
     * Try to find the optimal order to place, for satisfying the demanded value from a reserve asset.
     * If that one was found and placed, the expected incoming value is returned, otherwise None is returned
     */
    def tryToFindAGoodLiquidityProvidingOrder(demand: UniqueDemand): Option[NewLiquidityTransformationOrder] = {
      val bestReserveAssets: Option[Tuple2[Asset, Double]] =
        exchangeConfig.reserveAssets
          .filterNot(demand.dontUseTheseReserveAssets.contains)
          .filter(e => tradePairs.contains(TradePair(demand.asset, e)))
          .filter(e => estimatedManufactingCosts(demand, e) < wc.balanceSnapshot.get(e).map(_.amountAvailable).getOrElse(0.0)) // enough balance available?
          .map(e => (e,
            localExchangeRateRating(
              TradePair(demand.asset, e),
              Buy,
              demand.amount,
              c.setTxLimitAwayFromEdgeLimit,
              wc.ticker,
              wc.referenceTicker,
              wc.orderBook))) // join local exchange rate rating
          .filter(_._2 >= -c.maxAcceptableExchangeRateLossVersusReferenceTicker) // local rate acceptable?
          .sortBy(e => e._2)
          .lastOption

      if (bestReserveAssets.isEmpty) {
        log.debug(s"${Emoji.LookingDown}  [${exchangeConfig.name}] no good reserve asset found to satisfy $demand")
        None
      } else {
        log.debug(s"[${exchangeConfig.name}] found best usable reserve asset: ${bestReserveAssets.get._1}, " +
          s"rating=${formatDecimal(bestReserveAssets.get._2, 5)} for providing $demand")
        val orderAmount: Double = ceilToTxGranularity(demand.asset, demand.amount)
        val tradePair = TradePair(demand.asset, bestReserveAssets.get._1)
        val limit = determineRealisticLimit(tradePair, Buy, orderAmount, c.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
        val orderRequest = OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, tradePair, Buy, exchangeConfig.feeRate, orderAmount, limit)
        Some(NewLiquidityTransformationOrder(orderRequest))
      }
    }

    // aim: free available liquidity shall be available to fulfill the sum of demand
    def determineDemandedLiquidity(): Seq[UniqueDemand] = {
      val unlocked: Map[Asset, Double] = determineUnlockedBalance()
      // unsatisfied demand:
      wc.liquidityDemand.values
        .map(e =>
          UniqueDemand(
            e.tradePattern,
            e.asset,
            Math.max(0.0, e.amount - unlocked.getOrElse(e.asset, 0.0)),
            e.dontUseTheseReserveAssets,
            e.lastRequested) // missing coins
        )
        .filter(_.amount != 0.0)
        .toSeq
    }

    val demandedLiquidity: Seq[UniqueDemand] = determineDemandedLiquidity()
    if (demandedLiquidity.nonEmpty) {
      log.debug(s"[${exchangeConfig.name}] identified liquidity demand: $demandedLiquidity")
    }
    demandedLiquidity.flatMap { d =>
      tryToFindAGoodLiquidityProvidingOrder(d)
    }
  }


  def convertBackNotNeededNoneReserveLiquidityOrders(): Seq[NewLiquidityTransformationOrder] = {

    def reserveAssetsWithLowLiquidity(): Set[Asset] = {
      try {
        val fromWallet: Set[Asset] =
          wc.balanceSnapshot
            .filter(e => exchangeConfig.reserveAssets.contains(e._1))
            .filterNot(e => exchangeConfig.doNotTouchTheseAssets.contains(e._1))
            .filterNot(_._1.isFiat)
            .filter(_._1.canConvertTo(exchangeConfig.usdEquivalentCoin, tradePairs))
            .map(e => (e, CryptoValue(e._1, e._2.amountAvailable).convertTo(exchangeConfig.usdEquivalentCoin, wc.ticker))) // + USD value - we expect we can convert a reserve asset to USD-equivalent-coin always
            .filter(e => e._2.amount < c.minimumKeepReserveLiquidityPerAssetInUSD) // value below minimum asset liquidity value
            .map(_._1._1)
            .toSet

        val remainingReserveAssets = exchangeConfig.reserveAssets.filter(fromWallet.contains)
        fromWallet ++ remainingReserveAssets
      } catch {
        case _: NoSuchElementException =>
          throw new RuntimeException(s"Sorry, can't work with reserve assets in ${exchangeConfig.name} wallet, which cannot be converted to USD. This is the wallet: ${wc.balanceSnapshot}")
      }
    }

    def convertBackLiquidityTxActive(source: Asset): Boolean = {
      wc.activeLiquidityTx.values.exists(e =>
        e.orderRef.exchange == exchangeConfig.name &&
          ((e.orderRequest.tradeSide == Sell && e.orderRequest.tradePair.baseAsset == source && exchangeConfig.reserveAssets.contains(e.orderRequest.tradePair.quoteAsset)) ||
            (e.orderRequest.tradeSide == Buy && exchangeConfig.reserveAssets.contains(e.orderRequest.tradePair.baseAsset) && e.orderRequest.tradePair.quoteAsset == source))
      )
    }

    //  [Concept Reserve-Liquidity-Management] (copied from description above)
    //  - Unused (not locked or demanded) liquidity of a non-Reserve-Asset will be automatically converted to a Reserve-Asset.
    //    Which reserve-asset it will be, is determined by:
    //    - [non-loss-asset-filter] Filtering acceptable exchange-rates based on ticker on that exchange compared to a [ReferenceTicker]-value
    //    - [fill-up] Try to reach minimum configured balance of each reserve-assets in their order of preference
    //    - [play safe] Remaining value goes to first (highest prio) reserve-asset (having a acceptable exchange-rate)
    def convertBackToReserveAsset(cryptoValue: CryptoValue): Option[NewLiquidityTransformationOrder] = {
      val possibleReserveAssets: List[Tuple2[Asset, Double]] =
        exchangeConfig.reserveAssets
          .filter(e => tradePairs.contains(TradePair(cryptoValue.asset, e))) // filter available TradePairs only (with a local ticker)
          .map(e => (e, localExchangeRateRating(
            TradePair(cryptoValue.asset, e),
            Sell,
            cryptoValue.amount,
            c.setTxLimitAwayFromEdgeLimit,
            wc.ticker,
            wc.referenceTicker,
            wc.orderBook))) // add local exchange rate rating

      val availableReserveAssets: List[Tuple2[Asset, Double]] =
        possibleReserveAssets
          .filter(_._2 >= -c.maxAcceptableExchangeRateLossVersusReferenceTicker) // [non-loss-asset-filter]

      if (availableReserveAssets.isEmpty) {
        if (possibleReserveAssets.isEmpty) log.debug(s"[${exchangeConfig.name}] no reserve asset available to convert back $cryptoValue")
        else log.debug(s"[${exchangeConfig.name}] currently no reserve asset available with a good exchange-rate to convert back $cryptoValue. " +
          s"Available assets/ticker-rating: $availableReserveAssets")
        return None
      }

      val bestAvailableReserveAssets: List[Asset] =
        availableReserveAssets
          .sortBy(e => e._2) // sorted by local exchange rate rating
          .map(_._1) // asset only
          .reverse // best rating first

      val reserveAssetsNeedFillUp: Set[Asset] = reserveAssetsWithLowLiquidity()
      var destinationReserveAsset: Option[Asset] = bestAvailableReserveAssets.find(reserveAssetsNeedFillUp.contains) // [fill-up]
      if (destinationReserveAsset.isDefined) {
        log.info(s"${Emoji.ThreeBitcoin}  [${exchangeConfig.name}] transferring $cryptoValue back to reserve asset ${destinationReserveAsset.get} [fill-up]")
      }

      if (destinationReserveAsset.isEmpty) {
        destinationReserveAsset = Some(availableReserveAssets.head._1) // [play safe]
        log.info(s"${Emoji.ThreeBitcoin}  [${exchangeConfig.name}] transferring $cryptoValue back to reserve asset ${destinationReserveAsset.get} [primary sink]")
      }

      val tradePair = TradePair(cryptoValue.asset, destinationReserveAsset.get)
      val limit: Double = determineRealisticLimit(tradePair, Sell, cryptoValue.amount, c.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
      val orderRequest = OrderRequest(
        UUID.randomUUID(),
        None,
        exchangeConfig.name,
        tradePair,
        Sell,
        exchangeConfig.feeRate,
        ceilToTxGranularity(cryptoValue.asset, cryptoValue.amount),
        limit
      )
      Some(NewLiquidityTransformationOrder(orderRequest))
    }

    val freeUnusedNoneReserveAssetLiquidity: Map[Asset, Double] =
      determineUnlockedBalance()
        .filterNot(e => exchangeConfig.reserveAssets.contains(e._1)) // only select non-reserve-assets
        .filterNot(e => convertBackLiquidityTxActive(e._1)) // don't issue another liquidity tx for an asset, where another tx is still active
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
        .filter(_._1.canConvertTo(exchangeConfig.usdEquivalentCoin, tradePairs))
        .map(e => (e._1, e._2, CryptoValue(e._1, e._2).convertTo(exchangeConfig.usdEquivalentCoin, wc.ticker))) // join USDT value
        .filter(e => e._3.amount >= c.dustLevelInUSD) // ignore values below the dust level
        .map(e => (e._1, e._2))
        .toMap

    freeUnusedNoneReserveAssetLiquidity.flatMap { f =>
      convertBackToReserveAsset(CryptoValue(f._1, f._2))
    }.toSeq
  }

  def rebalanceReserveAssetsOrders(pendingIncomingReserveLiquidity: Seq[CryptoValue]): Seq[NewLiquidityTransformationOrder] = {

    def liquidityConversionPossibleBetween(a: Asset, b: Asset): Boolean = {
      tradePairs.contains(TradePair(a, b)) || tradePairs.contains(TradePair(b, a))
    }

    if (log.isTraceEnabled) log.trace(s"[${exchangeConfig.name}] re-balancing reserve asset wallet:${wc.balanceSnapshot} with pending incoming $pendingIncomingReserveLiquidity")
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

    val unableToConvert: Iterable[CryptoValue] = virtualReserveAssetsAggregated.filter(e => !e.canConvertTo(exchangeConfig.usdEquivalentCoin, wc.ticker))
    if (unableToConvert.nonEmpty) {
      log.warn(s"${Emoji.EyeRoll}  [${exchangeConfig.name}] Some reserve assets cannot be judged, because we cannot convert them to USD, so no liquidity transformation is possible for them: $unableToConvert")
    }

    // a bucket is a portion of an reserve asset having a specific value (value is configured in 'tx-granularity-in-usd')

    // all reserve assets, that need more value : Map(Asset -> liquidity buckets missing)
    var liquiditySinkBuckets: Map[Asset, Int] = virtualReserveAssetsAggregated
      .filter(_.canConvertTo(exchangeConfig.usdEquivalentCoin, wc.ticker))
      .map(e => (e.asset, e.convertTo(exchangeConfig.usdEquivalentCoin, wc.ticker).amount)) // amount in USD
      .filter(_._2 < c.minimumKeepReserveLiquidityPerAssetInUSD) // below min keep amount?
      .map(e => (e._1, ((c.minimumKeepReserveLiquidityPerAssetInUSD - e._2) / c.txValueGranularityInUSD).ceil.toInt)) // buckets needed
      .toMap
    // all reserve assets, that have extra liquidity to distribute : Map(Asset -> liquidity buckets available for distribution)
    var liquiditySourcesBuckets: Map[Asset, Int] = currentReserveAssetsBalance // we can take coin only from currently really existing balance
      .filter(_.canConvertTo(exchangeConfig.usdEquivalentCoin, wc.ticker))
      .map(e => (e.asset, e.convertTo(exchangeConfig.usdEquivalentCoin, wc.ticker).amount)) // amount in USD
      // having more than the minimum limit + 150% tx-granularity (to avoid useless there-and-back transfers because of exchange rate fluctuations)
      .filter(_._2 >= c.minimumKeepReserveLiquidityPerAssetInUSD + c.txValueGranularityInUSD * 1.5)
      // keep minimum reserve liquidity + 50% bucket value (we don't sell our last half-full extra bucket ^^^)
      .map(e => (e._1, ((e._2 - c.minimumKeepReserveLiquidityPerAssetInUSD - c.txValueGranularityInUSD * 0.5) / c.txValueGranularityInUSD).floor.toInt)) // buckets to provide
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
      if (log.isTraceEnabled) log.trace(s"[${exchangeConfig.name}] re-balance reserve assets: sources: $liquiditySourcesBuckets / sinks: $liquiditySinkBuckets")
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

        val tx: OrderRequest = TradePair(sinkAsset, sourceAsset.get) match {

          case tp if tradePairs.contains(tp) =>
            val tradePair = tp
            val tradeSide = Buy
            val baseAssetBucketValue: Double = CryptoValue(exchangeConfig.usdEquivalentCoin, c.txValueGranularityInUSD)
              .convertTo(tradePair.baseAsset, wc.ticker).amount

            val orderAmountBaseAsset = bucketsToTransfer * baseAssetBucketValue
            val limit = determineRealisticLimit(tradePair, tradeSide, orderAmountBaseAsset, c.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
            OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, tradePair, tradeSide, exchangeConfig.feeRate, orderAmountBaseAsset, limit)

          case tp if tradePairs.contains(tp.reverse) =>
            val tradePair = tp.reverse
            val tradeSide = Sell
            val quoteAssetBucketValue: Double = CryptoValue(exchangeConfig.usdEquivalentCoin, c.txValueGranularityInUSD)
              .convertTo(tradePair.quoteAsset, wc.ticker).amount

            val amountBaseAssetEstimate = bucketsToTransfer * quoteAssetBucketValue / wc.ticker(tradePair).priceEstimate
            val limit = determineRealisticLimit(tradePair, tradeSide, amountBaseAssetEstimate, c.setTxLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
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

    liquidityTxOrders.map(NewLiquidityTransformationOrder)
  }


  def balanceLiquidity(): Unit = {

    def placeLiquidityOrders(orders: Seq[NewLiquidityTransformationOrder]): Future[Seq[OrderRef]] = {
      if (orders.isEmpty) return Future.successful(Nil)
      implicit val timeout: Timeout = config.global.internalCommunicationTimeout

//      try {
        Future.sequence(
          orders.map(o => (tradeRoom ? o).mapTo[Option[OrderRef]])
        ).map(_.flatten)
//      } catch {
//        case e: Exception => log.error(s"[${exchangeConfig.name}] Error while placing liquidity tx orders", e)
//      }
    }

    def waitUntilLiquidityOrdersFinished(orderRefs: Seq[OrderRef]): Unit = {
      if (orderRefs.isEmpty) return
      val orderFinishDeadline = Instant.now.plus(config.tradeRoom.maxOrderLifetime.multipliedBy(2))
      var stillUnfinished: Seq[OrderRef] = null
      breakable {
        do {
          stillUnfinished = Await.result(
            unfinishedOrders(orderRefs),
            config.global.internalCommunicationTimeout.duration.plus(500.millis)
          )
          if (stillUnfinished.isEmpty) break
          Thread.sleep(200)
        } while (Instant.now.isBefore(orderFinishDeadline))
      }
      if (stillUnfinished.isEmpty) {
        log.debug(s"[${exchangeConfig.name}] all ${orderRefs.size} liquidity transaction(s) finished")
      } else {
        throw new RuntimeException(s"Not all liquidity tx orders did finish. Still unfinished: [$stillUnfinished]") // should not happen, because TradeRoom/Exchange cleanup unfinsihed orders by themself!
      }
    }

    try {
      val provideDemandOrders: Seq[NewLiquidityTransformationOrder] = provideDemandedLiquidityOrders()
      val incomingReserveLiquidityOrders: Seq[NewLiquidityTransformationOrder] = convertBackNotNeededNoneReserveLiquidityOrders()
      val totalIncomingReserveLiquidity = (incomingReserveLiquidityOrders ++ provideDemandOrders)
        .map(_.orderRequest.calcIncomingLiquidity)
        .map(_.cryptoValue)
        .filter(e => exchangeConfig.reserveAssets.contains(e.asset))

      val rebalanceReservesOrders = rebalanceReserveAssetsOrders(totalIncomingReserveLiquidity)

      placeLiquidityOrders(provideDemandOrders ++ incomingReserveLiquidityOrders ++ rebalanceReservesOrders).onComplete {
        case Success(orderRefs) => waitUntilLiquidityOrdersFinished(orderRefs)
        case Failure(e: OrderBookTooFlatException) =>
          log.warn(s"[to be improved] [${exchangeConfig.name}] Cannot perform liquidity housekeeping because the order book " +
            s"of trade pair ${e.tradePair} was too flat")
        case Failure(e) =>
          log.error(s"[${exchangeConfig.name}] liquidity houskeeping failed", e)
      }
    } finally {
      parent ! Finished()
      self ! PoisonPill
    }
  }

  override def preStart(): Unit = {
    self ! Run()
  }

  override def receive: Receive = {
    case Run() => balanceLiquidity()
  }
}
