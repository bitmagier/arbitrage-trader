package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorSystem, OneForOneStrategy, Props, Status}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt


object RootGuardian {
  def props(): Props = Props(new RootGuardian())
}
class RootGuardian extends Actor {
  private val log: Logger = LoggerFactory.getLogger(classOf[RootGuardian])

  override val supervisorStrategy: OneForOneStrategy = {
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 20.minutes, loggingEnabled = true) {
      case _ => Restart
    }
  }

  private val tradeRoom = context.actorOf (TradeRoom.props (Config.tradeRoom), "TradeRoom")

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}


object Main extends App {
  lazy val actorSystem = ActorSystem("ArbitrageTrader")
  private val rootGuardian = actorSystem.actorOf(RootGuardian.props(), "RootGuardian")
}
