package org.purevalue.arbitrage.traderoom

import java.util.UUID

import org.purevalue.arbitrage.traderoom.Asset.BTC
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OrderSpec extends TestSuite
  with AnyWordSpecLike
  with Matchers {

  Asset.register("ALGO", "Algorand", isFiat = false)

  // from cointracking.freshdesk.com/en/support/solutions/articles/29000021505-bnb-balance-wrong-due-to-fees-not-being-deducted-
  // In case of a sell, the fee needs to be entered as additional amount on the sell side.
  // In case of a buy, the fee needs to be subtracted from the buy side.

  "OrderBill" must {
    "calculate correct balance sheet of a Buy trannsaction" in {
      val balance: Seq[LocalCryptoValue] = OrderBill.calcBalanceSheet(
        OrderRequest(
          UUID.randomUUID(),
          null,
          "e1",
          TradePair(Asset("ALGO"), BTC),
          TradeSide.Buy,
          0.01,
          100.0,
          0.42
        ))

      balance should have size 2
      balance should contain allOf (LocalCryptoValue("e1", BTC, -0.42 * 100.0), LocalCryptoValue("e1", Asset("ALGO"), 100.0 * 0.99))
    }

    "calculate correct balance sheet of a Sell trannsaction" in {
      val balance: Seq[LocalCryptoValue] = OrderBill.calcBalanceSheet(
        OrderRequest(
          UUID.randomUUID(),
          null,
          "e1",
          TradePair(Asset("ALGO"), BTC),
          TradeSide.Sell,
          0.01,
          100.0,
          0.42
        ))

      balance should have size 2
      balance should contain allOf (LocalCryptoValue("e1", BTC, 0.42 * 100.0), LocalCryptoValue("e1", Asset("ALGO"), -100.0 * 1.01))
    }
  }
}
