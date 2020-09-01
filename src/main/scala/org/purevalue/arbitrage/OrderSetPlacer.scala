package org.purevalue.arbitrage

import akka.actor.{Actor, ActorRef, Props, Status}
import org.purevalue.arbitrage.ExchangeAccountDataManager.{CancelOrder, CancelOrderResult, NewLimitOrder, NewOrderAck}
import org.slf4j.LoggerFactory

object OrderSetPlacer {
  def props(exchanges: Map[String, ActorRef]): Props = Props(new OrderSetPlacer(exchanges))
}
case class OrderSetPlacer(exchanges: Map[String, ActorRef]) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[OrderSetPlacer])
  private var numRequests: Int = _
  private var numFailures: Int = 0
  private var answers: List[NewOrderAck] = List()
  private var requestSender: ActorRef = _

  override def receive: Receive = {
    case requests: List[NewLimitOrder] =>
      requestSender = sender()
      numRequests = requests.length
      requests.foreach { o =>
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
          requestSender ! answers
        }
      }

    case c: CancelOrderResult =>
      if (c.success) log.info(s"$c")
      else log.warn(s"$c")

    case Status.Failure(e) =>
      log.warn(s"MultipleOrderPlacer received a failure: ${e.getMessage}")
      numFailures += 1
  }
}
