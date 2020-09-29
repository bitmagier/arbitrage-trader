package org.purevalue.arbitrage.util

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

case class WaitingFor() {
  private val latch: CountDownLatch = new CountDownLatch(1)

  def arrived(): Unit = latch.countDown()

  def isArrived: Boolean = latch.getCount == 0

  def await(maxWaitTime: FiniteDuration): Unit = {
    val arrived = latch.await(maxWaitTime.toMillis, TimeUnit.MILLISECONDS)
    if (!arrived) throw new TimeoutException("Maximum wait time exceeded")
  }
}