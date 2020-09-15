package org.purevalue.arbitrage

import java.time.Duration

import akka.Done
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, CoordinatedShutdown, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Main.actorSystem
import org.purevalue.arbitrage.traderoom.TradeRoom
import org.purevalue.arbitrage.traderoom.TradeRoom.Stop
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

  val tradeRoom: ActorRef = context.actorOf(TradeRoom.props(Config.tradeRoom), "TradeRoom")

  override def receive: Receive = {
    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }

  CoordinatedShutdown(actorSystem).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "shutdown traderoom") { () =>
    val shutdownTimeout: Duration = Config.gracefulShutdownTimeout
    implicit val askTimeout: Timeout = Timeout.create(shutdownTimeout)
    log.info("initiating graceful shutdown")
    tradeRoom.ask(Stop(shutdownTimeout.minusSeconds(1))).mapTo[Done]
  }
}


object Main extends App {
  lazy val actorSystem = ActorSystem("ArbitrageTrader")
  private val rootGuardian = actorSystem.actorOf(RootGuardian.props(), "RootGuardian")
}
