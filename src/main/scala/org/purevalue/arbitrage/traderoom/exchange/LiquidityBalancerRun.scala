package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.traderoom.TradeRoom.{GetFinishedLiquidityTxs, NewLiquidityTransformationOrder, OrderRef}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.WorkingContext
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancerRun.{Finished, Run}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.OrderBookTooFlatException
import org.purevalue.arbitrage.traderoom.{TradePair, TradeRoom}
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success}


object LiquidityBalancerRun {

  private case class Run()
  case class Finished()

  def props(config: Config,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            parent: ActorRef,
            tradeRoom: ActorRef,
            wc: WorkingContext): Props =
    Props(new LiquidityBalancerRun(config, exchangeConfig, tradePairs, parent, tradeRoom, wc))
}

// Temporary Actor - stops itself after successful run
class LiquidityBalancerRun(val config: Config,
                           val exchangeConfig: ExchangeConfig,
                           val tradePairs: Set[TradePair],
                           val parent: ActorRef,
                           val tradeRoom: ActorRef,
                           val wc: WorkingContext) extends Actor with ActorLogging {

  private implicit val actorSystem: ActorSystem = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  if (wc.liquidityLocks.values.exists(_.exchange != exchangeConfig.name)) throw new IllegalArgumentException

  def placeOrders(orders: Iterable[NewLiquidityTransformationOrder]): Future[Iterable[OrderRef]] = {
    if (orders.isEmpty) return Future.successful(Nil)
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout

    Future.sequence(
      orders.map(o =>
        (tradeRoom ? o).mapTo[Option[OrderRef]]
          .recover {
            case e: Exception =>
              log.error(e, s"[${exchangeConfig.name}] Error while placing liquidity tx order $o")
              None
          }
      )
    ).map(_.flatten)
  }

  def unfinishedOrders(refs: Iterable[OrderRef]): Future[Iterable[OrderRef]] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    (tradeRoom ? GetFinishedLiquidityTxs()).mapTo[Set[OrderRef]]
      .map(l => refs.filterNot(e => l.contains(e)))
  }

  def waitUntilLiquidityOrdersFinished(orderRefs: Iterable[TradeRoom.OrderRef]): Unit = {
    if (orderRefs.isEmpty) return

    val orderFinishDeadline = Instant.now.plus(config.tradeRoom.maxOrderLifetime.multipliedBy(2))
    var stillUnfinished: Iterable[OrderRef] = null
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


  def balanceLiquidity(): Unit = {
    try {
      placeOrders(
        new LiquidityBalancer(config, exchangeConfig, tradePairs, wc).calculateOrders()
      ).onComplete {
        // @formatter:off
        case Success(orderRefs)                    => waitUntilLiquidityOrdersFinished(orderRefs)
        case Failure(e: OrderBookTooFlatException) => log.warning(s"[to be improved] [${exchangeConfig.name}] Cannot perform liquidity housekeeping because the order book of trade pair ${e.tradePair} was too flat")
        case Failure(e)                            => log.error(e, s"[${exchangeConfig.name}] liquidity houskeeping failed")
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
    // @formatter:off
    case Run()                            => balanceLiquidity()
    case akka.actor.Status.Failure(cause) => log.error(cause, "Failure received")
    // @formatter:on
  }
}
