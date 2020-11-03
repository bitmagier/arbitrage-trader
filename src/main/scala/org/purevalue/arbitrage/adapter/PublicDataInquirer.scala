package org.purevalue.arbitrage.adapter

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem}
import org.purevalue.arbitrage.traderoom.TradePair
import org.purevalue.arbitrage.{Main, UserRootGuardian}

import scala.concurrent.ExecutionContext

object PublicDataInquirer {
  trait Command
  case class GetAllTradePairs(replyTo: ActorRef[Set[TradePair]]) extends Command
}
abstract class PublicDataInquirer(context: ActorContext[PublicDataInquirer.Command]) extends AbstractBehavior[PublicDataInquirer.Command](context) {
  implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  implicit val executionContext: ExecutionContext = system.executionContext
}
