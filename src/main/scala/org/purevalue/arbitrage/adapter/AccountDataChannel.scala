package org.purevalue.arbitrage.adapter

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem}
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom.exchange.Exchange
import org.purevalue.arbitrage.traderoom.{OrderRequest, TradePair}
import org.purevalue.arbitrage.{Main, UserRootGuardian}

import scala.concurrent.{ExecutionContext, Future}


object AccountDataChannel {
  trait Command
  case class ConnectionClosed(component: String) extends Command
  case class NewLimitOrder(orderRequest: OrderRequest, replyTo: ActorRef[Exchange.NewOrderAck]) extends Command
  case class CancelOrder(ref: OrderRef, replyTo: Option[ActorRef[Exchange.CancelOrderResult]]) extends Command
}
abstract class AccountDataChannel(context: ActorContext[AccountDataChannel.Command]) extends AbstractBehavior[AccountDataChannel.Command](context) {

  import AccountDataChannel._

  implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  implicit val executionContext: ExecutionContext = system.executionContext

  def cancelOrder(pair: TradePair, externalOrderId: String): Future[Exchange.CancelOrderResult]

  def handleCancelOrder(c: CancelOrder): Unit = {
    cancelOrder(c.ref.pair, c.ref.externalOrderId).foreach { result =>
      c.replyTo match {
        case Some(replyTo) => replyTo ! result
        case None if result.success => context.log.info(s"order successfully cancelled ${c.ref}")
        case None => context.log.info(s"order cancel failed: {} {}",
          if (result.orderUnknown) "(order unknown)" else "",
          result.text match {
            case Some(text) => text
            case None => ""
          })
      }
    }
  }
}
