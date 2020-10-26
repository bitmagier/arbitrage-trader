package org.purevalue.arbitrage.traderoom.exchange

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.purevalue.arbitrage.traderoom.Asset.BTC
import org.purevalue.arbitrage.traderoom.{Asset, TradePair, TradeSide}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OrderLimitChooserSpec extends TestKit(ActorSystem("ExchangeLiquidityManagerSpec"))
  with ImplicitSender
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  "OrderLimitChooser" must {
    "use ticker, if no order book is present" in {

      val limitChooser = new OrderLimitChooser(
        None,
        Ticker("e1", TradePair(BTC, Asset("USDT")), 9999.0, None, 10001.0, None, Some(10000.0))
      )

      limitChooser.determineRealisticOrderLimit(TradeSide.Buy, 1.0, 1.0, 0.0)
        .get shouldBe 10000.0 +- 0.000001
    }

    "find optimal limit for Sell and Buy" in {
      val book = OrderBook("e1", TradePair(BTC, Asset("USDT")),
        Map(
          10000.1 -> Bid(1000.1, 100),
          10000.0 -> Bid(10000.0, 2),
          9999.1 -> Bid(9999.1, 3),
          9998.0 -> Bid(9998.0, 20)),
        Map(10000.2 -> Ask(10000.2, 1),
          10000.6 -> Ask(10000.6, 2),
          10002.0 -> Ask(10002.0, 20),
          10002.1 -> Ask(10002.1, 100)
        )
      )

      val limitChooser = new OrderLimitChooser(Some(book), null)

      limitChooser.determineRealisticOrderLimit(TradeSide.Buy, 0.5, 1.0, 0.0) shouldBe Some(10000.2)
      limitChooser.determineRealisticOrderLimit(TradeSide.Buy, 2.0, 1.0, 0.0) shouldBe Some(10000.6)
      limitChooser.determineRealisticOrderLimit(TradeSide.Buy, 20, 1.0, 0.0) shouldBe Some(10002.0)
      limitChooser.determineRealisticOrderLimit(TradeSide.Buy, 500, 1.0, 0.0) shouldBe None

      limitChooser.determineRealisticOrderLimit(TradeSide.Sell, 0.5, 1.0, 0.0) shouldBe Some(10000.0)
      limitChooser.determineRealisticOrderLimit(TradeSide.Sell, 2.0, 1.0, 0.0) shouldBe Some(10000.0)
      limitChooser.determineRealisticOrderLimit(TradeSide.Sell, 20, 1.0, 0.0) shouldBe Some(9998.0)
      limitChooser.determineRealisticOrderLimit(TradeSide.Sell, 500, 1.0, 0.0) shouldBe None
    }
  }
}
