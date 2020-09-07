package org.purevalue.arbitrage

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Status}
import org.purevalue.arbitrage.ExchangeAccountDataManager.{CancelOrder, CancelOrderResult, NewLimitOrder, NewOrderAck}
import org.purevalue.arbitrage.OrderSetPlacer.NewOrderSet
import org.slf4j.LoggerFactory

/**
 * Places all orders in parallel.
 * If one order placement fails, it tries to immediately cancel the successful ones
 */
object OrderSetPlacer {
  case class NewOrderSet(orders:Seq[NewLimitOrder])

  def props(exchanges: Map[String, ActorRef]): Props = Props(new OrderSetPlacer(exchanges))
}
case class OrderSetPlacer(exchanges: Map[String, ActorRef]) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OrderSetPlacer])
  private var numRequests: Int = _
  private var numFailures: Int = 0
  private var answers: List[NewOrderAck] = List()
  private var requestSender: ActorRef = _
  private var expectedCancelOrderResults: Int = 0

  override def receive: Receive = {
    case request: NewOrderSet =>
      requestSender = sender()
      numRequests = request.orders.length
      request.orders.foreach { o =>
        exchanges(o.o.exchange) ! o
      }

    case a: NewOrderAck =>
      answers = a :: answers
      if (answers.length + numFailures == numRequests) {
        if (numFailures == 0) {
          requestSender ! answers
        } else { // try to cancel the other orders
          answers.foreach { e =>
            exchanges(e.exchange) ! CancelOrder(e.tradePair, e.externalOrderId)
          }
          expectedCancelOrderResults = answers.size
          requestSender ! answers
        }
        if (expectedCancelOrderResults == 0) { // nothing more to do here
          self ! PoisonPill
        }
      }

    case c: CancelOrderResult =>
      expectedCancelOrderResults -= 1
      if (c.success) log.info(s"$c")      else log.warn(s"$c")
      if (expectedCancelOrderResults == 0) { // nothing more to do here
        self ! PoisonPill
      }

    case Status.Failure(e) =>
      log.warn(s"MultipleOrderPlacer received a failure: ${e.getMessage}")
      numFailures += 1
  }
}
