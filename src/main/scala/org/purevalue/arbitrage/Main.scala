package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, ActorSystem, AllForOneStrategy, Props}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator
import org.purevalue.arbitrage.util.ExchangeDataOutdated

import scala.concurrent.duration.DurationInt


object UserRootGuardian {
  def props(config: Config): Props = Props(new UserRootGuardian(config))
}
class UserRootGuardian(val config: Config) extends Actor {
  val tradeRoomInitCoordinator: ActorRef = context.actorOf(TradeRoomInitCoordinator.props(config, self), "TradeRoomInitCoordinator")
  var tradeRoom: ActorRef = _

  /*
  From Akka 2.5 Documentation:

  The precise sequence of events during a restart is the following:

  - suspend the actor (which means that it will not process normal messages until resumed), and recursively suspend all children
  - call the old instance’s preRestart hook (defaults to sending termination requests to all children and calling postStop)
  - wait for all children which were requested to terminate (using context.stop()) during preRestart to actually terminate; this—like all actor operations—is non-blocking, the termination notice from the last killed child will effect the progression to the next step
  - create new actor instance by invoking the originally provided factory again
  - invoke postRestart on the new instance (which by default also calls preStart)
  - send restart request to all children which were not killed in step 3; restarted children will follow the same process recursively, from step 2
  - resume the actor
*/
  override val supervisorStrategy: AllForOneStrategy = {
    AllForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 2.minutes, loggingEnabled = true) {
      // @formatter:off
      case _: ExchangeDataOutdated => Restart
      case _: Exception            => Escalate
    } // @formatter:on
  }

  override def receive: Receive = {
    case TradeRoomInitCoordinator.InitializedTradeRoom(tradeRoom) => this.tradeRoom = tradeRoom
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

  private val userRootGuardian = actorSystem.actorOf(UserRootGuardian.props(config()), "RootGuardian")
}
