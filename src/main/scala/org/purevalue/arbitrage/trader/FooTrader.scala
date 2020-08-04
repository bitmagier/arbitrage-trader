package org.purevalue.arbitrage.trader

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import com.typesafe.config.Config
import org.purevalue.arbitrage.Main
import org.purevalue.arbitrage.TradeRoom._
import org.purevalue.arbitrage.trader.FooTrader.Trigger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationInt, FiniteDuration}


object FooTrader {
  case class Trigger()

  def props(config: Config, tradeRoom: ActorRef): Props = Props(new FooTrader(config, tradeRoom))
}

/**
 * A basic trader to evolve the concept
 */
class FooTrader(config: Config, tradeRoom: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[FooTrader])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val maxOpenOrderBundles: Int = config.getInt("max-open-order-bundles")
  var pendingOrderBundles: Map[UUID, OrderBundle] = Map()
  var activeOrderBundles: Map[UUID, OrderBundle] = Map()

  val scheduleRate:FiniteDuration = FiniteDuration(config.getDuration("schedule-rate").toNanos, TimeUnit.NANOSECONDS)

  val schedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(0.seconds, scheduleRate, self, Trigger())

  def findBestShot(t: TradableAssets): Option[OrderBundle] = ???

  override def receive: Receive = {
    case Trigger =>
      if (pendingOrderBundles.size < maxOpenOrderBundles)
        tradeRoom ! GetTradableAssets()

    case t: TradableAssets =>
      findBestShot(t) match {
        case Some(orderBundle) =>
          pendingOrderBundles += (orderBundle.id -> orderBundle)
          tradeRoom ! orderBundle
        case None =>
      }

    case OrderBundlePlaced(orderBundleId) =>
      val ob = pendingOrderBundles(orderBundleId)
      pendingOrderBundles = pendingOrderBundles - orderBundleId
      activeOrderBundles += (orderBundleId -> ob)
      log.info(s"FooTrader: $ob")

    case OrderBundleCompleted(ob) =>
      activeOrderBundles -= ob.orderBundle.id
      log.info(s"FooTrader: $ob")

    case Status.Failure(cause) => log.error("received failure", cause)
  }
}
