package org.purevalue.arbitrage.adapter

import akka.actor.typed.ActorRef
import org.purevalue.arbitrage.traderoom.OrderRequest
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom.exchange.Exchange

object AccountDataChannel {
  trait Command
  case class ConnectionClosed(component:String) extends Command
  case class NewLimitOrder(orderRequest: OrderRequest, replyTo: ActorRef[Exchange.NewOrderAck]) extends Command
  case class CancelOrder(ref: OrderRef, replyTo: ActorRef[Exchange.CancelOrderResult]) extends Command
}
abstract class AccountDataChannel {
}
