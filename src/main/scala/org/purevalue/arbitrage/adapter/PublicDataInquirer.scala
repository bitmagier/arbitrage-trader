package org.purevalue.arbitrage.adapter

import akka.actor.typed.ActorRef
import org.purevalue.arbitrage.traderoom.exchange.Exchange

object PublicDataInquirer {
  sealed trait Command
  case class GetAllTradePairs(replyTo: ActorRef[Exchange.Message]) extends Command
}
abstract class PublicDataInquirer {
}
