package org.purevalue.arbitrage.traderoom.adapter

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.purevalue.arbitrage.adapter.{Ask, Bid, OrderBook}
import org.purevalue.arbitrage.traderoom.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.traderoom.{TradePair, TradeSide}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OrderBookSpec extends TestKit(ActorSystem("ExchangeLiquidityManagerSpec"))
  with ImplicitSender
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {
  "OrderBook" must {
    "find optimal limit for Sell and Buy" in {
      val book = OrderBook("e1", TradePair(Bitcoin, USDT),
        Map(10000.0 -> Bid(10000.0, 2),
          9999.1 -> Bid(9999.1, 3),
          9998.0 -> Bid(9998.0, 20)),
        Map(10000.2 -> Ask(10000.2, 1),
          10000.6 -> Ask(10000.6, 2),
          10002.0 -> Ask(10002.0, 20))
      )

      book.determineOptimalOrderLimit(TradeSide.Buy, 0.5) shouldBe Some(10000.2)
      book.determineOptimalOrderLimit(TradeSide.Buy, 2.0) shouldBe Some(10000.6)
      book.determineOptimalOrderLimit(TradeSide.Buy, 20) shouldBe Some(10002.0)
      book.determineOptimalOrderLimit(TradeSide.Buy, 50) shouldBe None

      book.determineOptimalOrderLimit(TradeSide.Sell, 0.5) shouldBe Some(10000.0)
      book.determineOptimalOrderLimit(TradeSide.Sell, 2.0) shouldBe Some(10000.0)
      book.determineOptimalOrderLimit(TradeSide.Sell, 20) shouldBe Some(9998.0)
      book.determineOptimalOrderLimit(TradeSide.Sell, 50) shouldBe None
    }
  }
}
