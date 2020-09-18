package org.purevalue.arbitrage.util

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class UtilSpec extends TestSuite
  with AnyWordSpecLike
  with Matchers {

  "Util.formatDecimalWithPrecision" must {
    "format precisely" in {
      Util.formatDecimalWithPrecision(1.0, 8) shouldBe "1.0000000"
      Util.formatDecimalWithPrecision(1.0, 4) shouldBe "1.000"
      Util.formatDecimalWithPrecision(11755.938472987563254, 8) shouldBe "11755.938"
      Util.formatDecimalWithPrecision(11755.93, 8) shouldBe "11755.930"
      Util.formatDecimalWithPrecision(0.0384235, 8) shouldBe "0.0384235"
      Util.formatDecimalWithPrecision(-0.0384235, 8) shouldBe "-0.0384235"
      Util.formatDecimalWithPrecision(0.038423, 8) shouldBe "0.0384230"
      Util.formatDecimalWithPrecision(0.03842351, 8) shouldBe "0.0384235"
    }
  }
}
