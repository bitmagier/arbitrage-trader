package org.purevalue.arbitrage

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.purevalue.arbitrage.traderoom.{TradeRoom, TradeRoomInitCoordinator}


object UserRootGuardian {
  var tradeRoomInitCoordinator: ActorRef[TradeRoomInitCoordinator.Reply] = _
  var tradeRoom: ActorRef[TradeRoom.Message] = _

  def apply(config: Config): Behavior[UserRootGuardian.Reply] =
    Behaviors.setup {
      context =>
        tradeRoomInitCoordinator = context.spawn(TradeRoomInitCoordinator(config, context.self), "TradeRoomInitCoordinator")

        Behaviors.receiveMessage {
          case TradeRoomInitialized(tradeRoom) =>
            this.tradeRoom = tradeRoom
            Behaviors.same
        }
    }

  sealed trait Reply
  case class TradeRoomInitialized(tradeRoom: ActorRef[TradeRoom.Message]) extends Reply

  // TODO
  //  override val supervisorStrategy: AllForOneStrategy = {
  //    AllForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 2.minutes, loggingEnabled = true) {
  //      // @formatter:off
//      case _: ActorKilledException => Restart
//      case _: Exception            => Escalate
//    } // @formatter:on
  //  }
}


object Main extends App {
  private val _config = Config.load()
  private val _actorSystem: ActorSystem[UserRootGuardian.Reply] = ActorSystem[UserRootGuardian.Reply](UserRootGuardian(config()), "ArbitrageTrader")

  def config(): Config = _config
  def actorSystem: ActorSystem[UserRootGuardian.Reply] = _actorSystem
}


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
