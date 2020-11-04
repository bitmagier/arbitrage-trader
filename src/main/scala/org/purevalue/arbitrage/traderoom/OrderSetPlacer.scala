package org.purevalue.arbitrage.traderoom

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, MessageAdaptionFailure, Signal, Terminated}
import akka.util.Timeout
import org.purevalue.arbitrage.traderoom.OrderSetPlacer.NewOrderSet
import org.purevalue.arbitrage.traderoom.exchange.Exchange
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{CancelOrder, NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.{GlobalConfig, Main, UserRootGuardian}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/**
 * Places all orders in parallel.
 * If one order placement fails, it tries to immediately cancel the successful ones
 */
object OrderSetPlacer {
  def apply(globalConfig: GlobalConfig,
            exchanges: Map[String, ActorRef[Exchange.Message]]):
  Behavior[NewOrderSet] =
    Behaviors.setup(context => new OrderSetPlacer(context, globalConfig, exchanges))

  sealed trait Message
  case class NewOrderSet(orders: Seq[OrderRequest], replyTo: ActorRef[Seq[NewOrderAck]]) extends Message
}
class OrderSetPlacer(context: ActorContext[NewOrderSet],
                     globalConfig: GlobalConfig,
                     exchanges: Map[String, ActorRef[Exchange.Message]]) extends AbstractBehavior[NewOrderSet](context) {

  private var ackCollector: Option[ActorRef[NewOrderAck]] = None

  override def onMessage(message: NewOrderSet): Behavior[NewOrderSet] =
    message match {
      case request: NewOrderSet =>
        val numRequests = request.orders.length
        ackCollector = Some(context.spawn(AckCollector(globalConfig, exchanges, numRequests, request.replyTo), s"${context.self.path.name}-AckCollector"))
        request.orders.foreach { o =>
          exchanges(o.exchange) ! NewLimitOrder(o, ackCollector.get)
        }
        context.watch(ackCollector.get)
        Behaviors.unhandled
    }

  override def onSignal: PartialFunction[Signal, Behavior[NewOrderSet]] = {
    case Terminated(_) =>
      Behaviors.stopped
  }
}

object AckCollector {
  def apply(globalConfig: GlobalConfig,
            exchanges: Map[String, ActorRef[Exchange.Message]],
            numRequests: Int,
            reportTo: ActorRef[Seq[NewOrderAck]]):
  Behavior[NewOrderAck] =
    Behaviors.setup(context => new AckCollector(context, globalConfig, exchanges, numRequests, reportTo))
}
class AckCollector(context: ActorContext[NewOrderAck],
                   globalConfig: GlobalConfig,
                   exchanges: Map[String, ActorRef[Exchange.Message]],
                   numRequests: Int,
                   reportTo: ActorRef[Seq[NewOrderAck]]) extends AbstractBehavior[NewOrderAck](context) {
  private val log = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  implicit val executionContext: ExecutionContext = system.executionContext
  private var answers: List[NewOrderAck] = List()
  private var numFailures: Int = 0


  def onResponse(): Behavior[NewOrderAck] = {
    if (answers.length + numFailures < numRequests) {
      Behaviors.same
    } else {
      if (numFailures == 0) {
        reportTo ! answers
      } else { // try to cancel the other orders
        implicit val timeout: Timeout = globalConfig.httpTimeout.plus(500.millis)
        answers.foreach { e =>
          exchanges(e.exchange) ! CancelOrder(e.toOrderRef, None)
        }
        reportTo ! answers
      }
      Behaviors.stopped
    }
  }

  override def onMessage(message: NewOrderAck): Behavior[NewOrderAck] = message match {
    case a: NewOrderAck =>
      answers = a :: answers
      onResponse()
  }

  override def onSignal: PartialFunction[Signal, Behavior[NewOrderAck]] = {
    case MessageAdaptionFailure(exception) =>
      log.warn(s"OrderSetPlacer received a failure: ${exception.getMessage}")
      numFailures += 1
      onResponse()
  }
}