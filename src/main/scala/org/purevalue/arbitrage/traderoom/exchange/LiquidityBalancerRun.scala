package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.traderoom.TradeRoom.{FinishedLiquidityTx, GetFinishedLiquidityTxs, LiquidityTx, NewLiquidityTransformationOrder, OrderRef}
import org.purevalue.arbitrage.traderoom.TradeSide.{Buy, Sell}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancerRun.{Finished, Run, WorkingContext}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, OrderBookTooFlatException, UniqueDemand}
import org.purevalue.arbitrage.traderoom.{Asset, CryptoValue, OrderRequest, TradePair, TradeRoom, TradeSide}
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success}

trait LMStatisticEvent {
  def exchange: String
}
case class NoGoodManufacturingOptionAvailable(exchange: String, demandAsset: Asset) extends LMStatisticEvent
case class LiquidityRebalanceCurrentlyLossy(exchange: String, source: Asset, destination: Asset) extends LMStatisticEvent

object LMStats {
  private var stats: Map[LMStatisticEvent, Int] = Map()

  def inc(e: LMStatisticEvent): Unit = {
    stats.synchronized {
      stats = stats.updated(e, stats.getOrElse(e, 0))
    }
  }
}


object LiquidityBalancerRun {
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
           ): Props = Props(new LiquidityBalancerRun(config, exchangeConfig, tradePairs, parent, tradeRoom, wc))
}

// Temporary Actor - stops itself after successful run
class LiquidityBalancerRun(val config: Config,
                           val exchangeConfig: ExchangeConfig,
                           val tradePairs: Set[TradePair],
                           val parent: ActorRef,
                           val tradeRoom: ActorRef,
                           val wc: WorkingContext) extends Actor {

  private val log = LoggerFactory.getLogger(classOf[LiquidityBalancerRun])
  private implicit val actorSystem: ActorSystem = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  if (wc.liquidityLocks.values.exists(_.exchange != exchangeConfig.name)) throw new IllegalArgumentException

  def placeOrders(orders: Seq[NewLiquidityTransformationOrder]): Future[Seq[OrderRef]] = {
    if (orders.isEmpty) return Future.successful(Nil)
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout

    Future.sequence(
      orders.map(o =>
        (tradeRoom ? o).mapTo[Option[OrderRef]]
          .recover {
            case e: Exception =>
              log.error(s"[${exchangeConfig.name}] Error while placing liquidity tx order $o", e)
              None
          }
      )
    ).map(_.flatten)
  }

  def unfinishedOrders(refs: Seq[OrderRef]): Future[Seq[OrderRef]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    (tradeRoom ? GetFinishedLiquidityTxs()).mapTo[List[FinishedLiquidityTx]]
      .map(l =>
        refs.filter(e =>
          !l.exists(_.finishedOrder.ref == e)))
  }

  def waitUntilLiquidityOrdersFinished(orderRefs: Seq[TradeRoom.OrderRef]): Unit = {
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

  def createOrders(transfers: Seq[LiquidityTransfer]): Seq[NewLiquidityTransformationOrder] = {
    transfers.map(e => NewLiquidityTransformationOrder(
      OrderRequest(UUID.randomUUID(), None, exchangeConfig.name, e.pair, e.side, exchangeConfig.feeRate, e.quantity, e.limit))
    )
  }

  def determineUnlockedBalance(): Map[Asset, Double] =
    LiquidityManager.determineUnlockedBalance(wc.balanceSnapshot, wc.liquidityLocks, exchangeConfig)

  def txGranularity(asset: Asset): Double =
    CryptoValue(exchangeConfig.usdEquivalentCoin, config.liquidityManager.txValueGranularityInUSD)
      .convertTo(asset, wc.ticker)
      .amount

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
        val demandBuckets: Int = if (resultingDemand <= 0.0) 0 else (resultingDemand / txGranularity(asset)).round.toInt + 1
        (asset, demandBuckets)
    }
  }

  // (supply - max(demand, lock)) & greater-than-zero
  // supply-buckets = (amount / tx-granularity-of-asset).round - 1
  def calcPureSupplyOverhead: Map[Asset, Int] = {
    wc.balanceSnapshot.map {
      case (asset, balance) =>
        val demand: Double = wc.liquidityDemand.values.find(_.asset == asset).map(_.amount).getOrElse(0.0)
        val locked: Double = wc.liquidityLocks.values.flatMap(_.coins).filter(_.asset == asset).map(_.amount).sum
        val overheadAmount: Double = balance.amountAvailable - Math.max(demand, locked)
        val overheadBuckets: Int = Math.max(0, (overheadAmount / txGranularity(asset)).round.toInt - 1)
        (asset, overheadBuckets)
    }.filter(_._2 > 0.0)
  }

  case class LiquidityTransfer(supplyAsset: Asset, pair: TradePair, side: TradeSide, buckets: Int, quantity: Double, limit: Double, exchangeRateRating: Double)

  def tradepairAndSide(destination: Asset, source: Asset): (TradePair, TradeSide) = {
    // @formatter:off
    tradePairs.find(e => e.baseAsset == destination && e.quoteAsset == source) match {
      case Some(pair) => (pair, Buy)
      case None       => (TradePair(source, destination), Sell)
    } // @formatter:on
  }

  // determines remaining-supply = supply minus all transfers from that supply
  def calcRemainingSupply(supply: Map[Asset, Int], transfers: Iterable[LiquidityTransfer]): Map[Asset, Int] = {
    var result = supply
    for (t <- transfers) {
      if (result.contains(t.supplyAsset)) {
        val remainingBuckets = Math.max(0, result(t.supplyAsset) - t.buckets)
        if (remainingBuckets == 0) result = result - t.supplyAsset
        else result = result + (t.supplyAsset -> remainingBuckets)
      }
    }
    result
  }

  /**
   * Fill unsatisfied-demands (in order) using supply-overhead:
   * - [1] when there is a trade pair available
   * - [2] and when exchange-rate is acceptable
   * - [3] then use one with the best exchange rate rating
   * - [4] if multiple options are left, use non-reserve-assets first (any), then use reserve-assets (in reverse order)
   *
   * @return total number of possible transactions (in buckets) to satisfy the demand
   */
  def fillDemand(unsatisfiedDemand: List[(Asset, Int)], supplyOverhead: Map[Asset, Int]): List[LiquidityTransfer] = {
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
     * @return (remainingDemandBuckets:Int, remainingSupply:Map[Asset,Int], transfer:Option[LiquidityBucketTransfer])
     */
    def findTransfer(demandAsset: Asset, demandBuckets: Int, supply: Map[Asset, Int]): (Int, Map[Asset, Int], Option[LiquidityTransfer]) = {
      val transferOptions =
        supply
          .filter(_._1.canConvertDirectlyTo(demandAsset, tradePairs)) // transfer possible
          .map(e => {
            val (pair, side) = tradepairAndSide(demandAsset, e._1)
            val buckets: Int = Math.min(demandBuckets, e._2)
            val quantity: Double = buckets * txGranularity(pair.baseAsset)
            val limit = determineRealisticLimit(pair, side, quantity, config.liquidityManager.txLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
            val exchangeRateRating: Double = localExchangeRateRating(pair, side, limit, wc.referenceTicker)
            LiquidityTransfer(e._1, pair, side, buckets, quantity, limit, exchangeRateRating)
          })
          .filter(_.exchangeRateRating >= -config.liquidityManager.maxAcceptableExchangeRateLossVersusReferenceTicker) // rating above loss limit

      transferOptions match {
        case options if options.isEmpty =>
          LMStats.inc(NoGoodManufacturingOptionAvailable(exchangeConfig.name, demandAsset))

          (demandBuckets, supply, None)

        case transferOptions =>
          val bestRating = transferOptions.map(_.exchangeRateRating).max
          val BestRatingToleranceLevel = 0.00001
          val transfer = transferOptions
            .filter(_.exchangeRateRating >= bestRating - BestRatingToleranceLevel)
            .maxBy(e => sourceAssetPreference(e.supplyAsset))

          (demandBuckets - transfer.buckets, calcRemainingSupply(supply, Seq(transfer)), Some(transfer))
      }
    }

    /**
     * @return (remainingDemand, remainingSupply, transfer)
     */
    @tailrec
    def findNextTransfer(demand: List[(Asset, Int)], supply: Map[Asset, Int]): (List[(Asset, Int)], Map[Asset, Int], Option[LiquidityTransfer]) = {
      if (demand.isEmpty) (demand, supply, None)
      else findTransfer(demand.head._1, demand.head._2, supply) match {
        case (remainingDemandBuckets: Int, rs: Map[Asset, Int], Some(t: LiquidityTransfer)) => ((demand.head._1, remainingDemandBuckets) :: demand.tail, rs, Some(t))
        case (_, _, None) => findNextTransfer(demand.tail, supply)
      }
    }

    findNextTransfer(unsatisfiedDemand, supplyOverhead) match {
      case (_, _, None) => Nil
      case (remainingDemand, remainingSupply, Some(transfer)) => transfer :: fillDemand(remainingDemand, remainingSupply)
    }
  }

  // remaining final supply overhead with respect to minimumReserveKeepBuckets
  def calcFinalSupplyOverhead(supplyOverhead: Map[Asset, Int]): Map[Asset, Int] = {
    val minReserveAssetKeepBuckets: Int =
      (config.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD / config.liquidityManager.txValueGranularityInUSD).round.toInt

    supplyOverhead
      .filterNot(_._1 == exchangeConfig.primaryReserveAsset)
      .map { // @formatter:off
          case (asset, buckets) if exchangeConfig.reserveAssets.contains(asset) => (asset, Math.max(0, buckets - minReserveAssetKeepBuckets))
          case (asset, buckets)                                                 => (asset, buckets)
        } // @formatter:on
      .filter(_._2 != 0)
  }

  def roundToBuckets(asset:Asset, amount: Double): Int = {

  }

  def determineSecondaryReserveToFillUp(fillDemandTransfers: Iterable[LiquidityTransfer]): List[(Asset, Int)] = {
    var secondaryReserveBalanceBuckets: Map[Asset, Int] =
      exchangeConfig.reserveAssets.tail
        .map(e => e -> roundToBuckets(e, wc.balanceSnapshot.get(e).map(_.amountAvailable).getOrElse(0.0))
        .toMap

    for (t <- fillDemandTransfers) {

    }


  }

  def transferEverything(destination: Asset, overhead: Map[Asset, Int]): Iterable[LiquidityTransfer] = ???

  def balanceLiquidity(): Unit = {
    def squash(unsquashed: Iterable[LiquidityTransfer]): Iterable[LiquidityTransfer] = {
      unsquashed
        .groupBy(e => (e.supplyAsset, e.pair, e.side))
        .map { e =>
          val quantity = e._2.map(_.quantity).sum
          val limit = determineRealisticLimit(e._1._2, e._1._3, quantity, config.liquidityManager.txLimitAwayFromEdgeLimit, wc.ticker, wc.orderBook)
          LiquidityTransfer(e._1._1, e._1._2, e._1._3, e._2.map(_.buckets).sum, quantity, limit, 0.0) // rating is not important any more, we just merge already approved single transfers and find an appropriate limit
        }
    }

    try {
      val unsatisfiedDemand: List[(Asset, Int)] = calcUnsatisfiedDemand.toList.sortBy(_._2)
      val supplyOverhead: Map[Asset, Int] = calcPureSupplyOverhead
      val fillDemandTransfers: Iterable[LiquidityTransfer] = fillDemand(unsatisfiedDemand, supplyOverhead)

      val afterDemandsFilledSupplyOverhead: Map[Asset, Int] = calcFinalSupplyOverhead(calcRemainingSupply(supplyOverhead, fillDemandTransfers))
      val secondaryReserveToFillUp: List[(Asset, Int)] = determineSecondaryReserveToFillUp(fillDemandTransfers)
      val secondaryReserveFillUpTransfers: Iterable[LiquidityTransfer] = fillDemand(secondaryReserveToFillUp, afterDemandsFilledSupplyOverhead)

      val afterSecondaryReserveFilledSupplyOverhead: Map[Asset, Int] = calcRemainingSupply(afterDemandsFilledSupplyOverhead, secondaryReserveFillUpTransfers)
      val primaryReserveInflowTransfers: Iterable[LiquidityTransfer] = transferEverything(exchangeConfig.primaryReserveAsset, afterSecondaryReserveFilledSupplyOverhead)

      placeOrders(
        createOrders(
          squash(fillDemandTransfers ++ secondaryReserveFillUpTransfers ++ primaryReserveInflowTransfers).toSeq)).onComplete {
        // @formatter:off
        case Success(orderRefs)                    => waitUntilLiquidityOrdersFinished(orderRefs)
        case Failure(e: OrderBookTooFlatException) => log.warn(s"[to be improved] [${exchangeConfig.name}] Cannot perform liquidity housekeeping because the order book of trade pair ${e.tradePair} was too flat")
        case Failure(e)                            => log.error(s"[${exchangeConfig.name}] liquidity houskeeping failed", e)
        // @formatter:on
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
