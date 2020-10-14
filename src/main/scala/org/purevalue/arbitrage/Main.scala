package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorRef, ActorSystem, AllForOneStrategy, Props, Status}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator
import org.purevalue.arbitrage.util.RestartIntentionException
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt


object RootGuardian {
  def props(config: Config): Props = Props(new RootGuardian(config))
}
class RootGuardian(val config: Config) extends Actor {
  private val log: Logger = LoggerFactory.getLogger(classOf[RootGuardian])

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
    AllForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 20.minutes, loggingEnabled = true) {
      case e: RestartIntentionException => log.info(s"restart intended: ${e.getMessage}"); Restart
      case e: Throwable => log.warn(s"Stopping actor system, because:", e); Stop
    }
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
