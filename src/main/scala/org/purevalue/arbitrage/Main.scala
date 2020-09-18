package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Status}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt


object RootGuardian {
  def props(config: Config): Props = Props(new RootGuardian(config))
}
class RootGuardian(val config: Config) extends Actor {
  private val log: Logger = LoggerFactory.getLogger(classOf[RootGuardian])

  var initCoordinator: ActorRef = _
  var tradeRoom: ActorRef = _

  override val supervisorStrategy: OneForOneStrategy = {
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 20.minutes, loggingEnabled = true) {
      case _ => Restart
    }
  }

  override def preStart(): Unit = {
    initCoordinator = context.actorOf(TradeRoomInitCoordinator.props(config, self))
  }

  override def receive: Receive = {
    // @formatter:off
    case TradeRoomInitCoordinator.InitializedTradeRoom(tradeRoom) => this.tradeRoom = tradeRoom
    case Status.Failure(cause)                                    => log.error("received failure", cause)
    // @formatter:on
  }

//  // TODO coordinated shutdown
//  CoordinatedShutdown(actorSystem).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "shutdown traderoom") { () =>
//    val shutdownTimeout: Duration = config.gracefulShutdownTimeout
//    implicit val askTimeout: Timeout = Timeout.create(shutdownTimeout)
//    log.info("initiating graceful shutdown")
//    tradeRoom.ask(Stop(shutdownTimeout.minusSeconds(1))).mapTo[Done]
//  }
}


object Main extends App {
  lazy val actorSystem = ActorSystem("ArbitrageTrader")
  private val _config = Config.load()
  def config(): Config = _config

  private val rootGuardian = actorSystem.actorOf(RootGuardian.props(config()), "RootGuardian")
}
