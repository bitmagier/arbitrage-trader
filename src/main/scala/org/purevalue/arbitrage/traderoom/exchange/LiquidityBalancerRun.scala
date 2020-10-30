package org.purevalue.arbitrage.traderoom.exchange

import java.time.Instant

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.WorkingContext
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.OrderBookTooFlatException
import org.purevalue.arbitrage.traderoom.{OrderRequest, TradePair, TradeRoom}
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main, UserRootGuardian}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success}


object LiquidityBalancerRun {
  def apply(config: Config,
            exchangeConfig: ExchangeConfig,
            tradePairs: Set[TradePair],
            parent: ActorRef[Exchange.Message],
            tradeRoom: ActorRef[TradeRoom.Message],
            wc: WorkingContext):
  Behavior[Command] =
    Behaviors.setup(context => new LiquidityBalancerRun(context, config, exchangeConfig, tradePairs, parent, tradeRoom, wc))

  sealed trait Command
  private case class Run() extends Command
}

// Temporary Actor - stops itself after successful run
class LiquidityBalancerRun(context: ActorContext[LiquidityBalancerRun.Command],
                           config: Config,
                           exchangeConfig: ExchangeConfig,
                           tradePairs: Set[TradePair],
                           parent: ActorRef[Exchange.Message],
                           tradeRoom: ActorRef[TradeRoom.Message],
                           wc: WorkingContext) extends AbstractBehavior[LiquidityBalancerRun.Command](context) {

  import LiquidityBalancerRun._

  private implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = system.executionContext
  private implicit val timeout: Timeout = config.global.internalCommunicationTimeout

  if (wc.liquidityLocks.values.exists(_.exchange != exchangeConfig.name)) throw new IllegalArgumentException

  def placeOrders(orders: Iterable[OrderRequest]): Future[Iterable[OrderRef]] = {
    if (orders.isEmpty) return Future.successful(Nil)

    Future.sequence(
      orders.map(o =>
        tradeRoom.ask(ref => TradeRoom.PlaceLiquidityTransformationOrder(o, ref))
          .recover {
            case e: Exception =>
              context.log.error(s"[${exchangeConfig.name}] Error while placing liquidity tx order $o", e)
              None
          }
      )
    ).map(_.flatten)
  }

  def unfinishedOrders(refs: Iterable[OrderRef]): Future[Iterable[OrderRef]] = {
    tradeRoom.ask(ref => TradeRoom.GetFinishedLiquidityTxs(ref))
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
      context.log.debug(s"[${exchangeConfig.name}] all ${orderRefs.size} liquidity transaction(s) finished")
    } else { // should not happen, because TradeRoom/Exchange doing cleanup unfinished orders by themself!
      throw new RuntimeException(s"Not all liquidity tx orders did finish. Still unfinished: [$stillUnfinished]")
    }
  }

  def balanceLiquidity(): Unit = {
    placeOrders(
      new LiquidityBalancer(config, exchangeConfig, tradePairs, wc).calculateOrders()
    ).onComplete {
      // @formatter:off
      case Success(orderRefs)                    => waitUntilLiquidityOrdersFinished(orderRefs)
      case Failure(e: OrderBookTooFlatException) => context.log.warn(s"[code improved needed] [${exchangeConfig.name}] Cannot perform liquidity housekeeping because the order book of trade pair ${e.tradePair} was too flat")
      case Failure(e)                            => context.log.error(s"[${exchangeConfig.name}] liquidity houskeeping failed", e)
      // @formatter:on
    }
  }

  override def onMessage(message: Command): Behavior[Command] = {
    case Run() =>
      balanceLiquidity()
      parent ! Exchange.LiquidityBalancerRunFinished()
      Behaviors.stopped
  }
}
