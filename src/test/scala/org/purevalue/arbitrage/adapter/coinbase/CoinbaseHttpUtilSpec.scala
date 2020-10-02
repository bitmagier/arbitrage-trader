package org.purevalue.arbitrage.adapter.coinbase

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class CoinbaseHttpUtilSpec extends TestSuite with AnyWordSpecLike with Matchers {
  "CoinbaseHttpUtil" must {
    "parse server time" in {
      CoinbaseHttpUtil.parseServerTime("""{"iso":"2020-10-01T21:22:24Z","epoch":1601587344.1}""") shouldBe 1601587344.1
      CoinbaseHttpUtil.parseServerTime("""{"iso":"2020-10-01T21:22:24Z","epoch":1601587344.}""") shouldBe 1601587344.0
    }
  }
}
