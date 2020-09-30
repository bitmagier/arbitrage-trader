package org.purevalue.arbitrage.util

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class UtilSpec extends TestSuite
  with AnyWordSpecLike
  with Matchers {

  "Util" must {
    "formatDecimalWithPrecision" in {
      Util.formatDecimalWithFixPrecision(1.0, 8) shouldBe "1.0000000"
      Util.formatDecimalWithFixPrecision(1.0, 4) shouldBe "1.000"
      Util.formatDecimalWithFixPrecision(11755.938472987563254, 8) shouldBe "11755.938"
      Util.formatDecimalWithFixPrecision(11755.93, 8) shouldBe "11755.930"
      Util.formatDecimalWithFixPrecision(0.0384235, 8) shouldBe "0.0384235"
      Util.formatDecimalWithFixPrecision(-0.0384235, 8) shouldBe "-0.0384235"
      Util.formatDecimalWithFixPrecision(0.038423, 8) shouldBe "0.0384230"
      Util.formatDecimalWithFixPrecision(0.03842351, 8) shouldBe "0.0384235"
    }

    "formatDecimal" in {
      Util.formatDecimal(-0.0012333, 5) shouldBe "-0.00123"
    }

    "convert step size to fraction digits" in {
      Util.stepSizeToFractionDigits(0.01) shouldBe 2
      Util.stepSizeToFractionDigits(0.000001) shouldBe 6
      Util.stepSizeToFractionDigits(0.000002) shouldBe 6
      Util.stepSizeToFractionDigits(0.000005) shouldBe 6
    }
  }
}
