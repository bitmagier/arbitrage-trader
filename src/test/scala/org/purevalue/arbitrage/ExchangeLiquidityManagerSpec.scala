package org.purevalue.arbitrage

import java.time.Duration

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.purevalue.arbitrage.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.TradeRoom.{LiquidityTransformationOrder, ReferenceTickerReadonly, WalletUpdateTrigger}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers._
import scala.concurrent.duration.DurationInt

class ExchangeLiquidityManagerSpec
  extends TestKit(ActorSystem("ExchangeLiquidityManagerSpec"))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory {

  val config = LiquidityManagerConfig(List(USDT, Bitcoin, Asset("ETH")),
    Duration.ofSeconds(1),
    Duration.ofSeconds(1),
    providingLiquidityExtra = 0.02,
    maxAcceptableLocalTickerLossFromReferenceTicker = 0.01,
    minimumKeepReserveLiquidityPerAssetInUSDT = 50.0,
    txLimitBelowOrAboveBestBidOrAsk = 0.005,
    rebalanceTxGranularityInUSDT = 20.0)


  val fee = Fee("e1", 0.002, 0.002)
  val referenceTicker = ReferenceTickerReadonly(Map(
    TradePair.of(Asset("ALGO"), USDT) -> ExtendedTicker("e1", TradePair.of(Asset("ALGO"), USDT), 0.50, 1.0, 0.51, 1.0, 0.505, 1.0, 0.5),
    TradePair.of(Bitcoin, USDT) -> ExtendedTicker("e1", TradePair.of(Bitcoin, USDT), 9999.0, 1.0, 10001.0, 1.0, 10000.0, 1.0, 10000.0),
    TradePair.of(Asset("ETH"), USDT) -> ExtendedTicker("e1", TradePair.of(Asset("ETH"), USDT), 399.0, 1.0, 401.0, 1.0, 400.0, 1.0, 400.0),
    TradePair.of(Asset("ETH"), Bitcoin) -> ExtendedTicker("e1", TradePair.of(Asset("ETH"), Bitcoin), 399.0/10000.0, 1.0, 401.0/10000.0, 1.0, 400.0/10000.0, 1.0, 400.0/10000.0)
  ))

  val tickers: Map[TradePair, Ticker] = referenceTicker.values.map(e =>
    (e._1, Ticker("e1", e._1, e._2.highestBidPrice, None, e._2.lowestAskPrice, None, Some(e._2.lastPrice)))
  ).toMap

  val tpData = ExchangeTPDataReadonly(tickers, Map(), Map(), null)

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "ExchangeLiquidityManager" must {
    "rebalance reserve assets" in {
      val tradeRoom = TestProbe()
      val wallet: Wallet = Wallet(Map(
        Bitcoin -> Balance(Bitcoin, 1.0, 0.0),
        USDT -> Balance(USDT, 0.05, 0.0)
      ))
      val m = system.actorOf(ExchangeLiquidityManager.props(config, "e1", tradeRoom.ref, tpData, wallet, fee, referenceTicker))

      m ! WalletUpdateTrigger("e1") // trigger housekeeping

      val messages: Seq[LiquidityTransformationOrder] = tradeRoom.expectMsgAllClassOf(5.seconds, classOf[LiquidityTransformationOrder], classOf[LiquidityTransformationOrder])
      println(messages)
      messages should have length 2
      val tpEthBtc = TradePair.of(Asset("ETH"), Bitcoin)
      val tpBtcUsdt = TradePair.of(Bitcoin, USDT)
      assert(messages.map(_.orderRequest.tradePair) == Seq(tpEthBtc, tpBtcUsdt))

      val orderBtcUsdt = messages.find(_.orderRequest.tradePair == tpBtcUsdt).get.orderRequest
      orderBtcUsdt.tradeSide shouldBe TradeSide.Sell
      orderBtcUsdt.calcIncomingLiquidity.asset shouldBe USDT
      val expectedUsdtInflow: Double = (config.minimumKeepReserveLiquidityPerAssetInUSDT / config.rebalanceTxGranularityInUSDT).ceil * config.rebalanceTxGranularityInUSDT
      orderBtcUsdt.calcIncomingLiquidity.amount shouldEqual expectedUsdtInflow +- 0.0000001

      val orderEthBtc = messages.find(_.orderRequest.tradePair == tpEthBtc).get.orderRequest
      orderEthBtc.tradeSide shouldBe TradeSide.Buy
      val ethInflowUSDT = orderEthBtc.calcIncomingLiquidity.convertTo(USDT, tickers).map(_.amount).get
      val expectedEthInflowInUsdt = (config.minimumKeepReserveLiquidityPerAssetInUSDT / config.rebalanceTxGranularityInUSDT).ceil * config.rebalanceTxGranularityInUSDT
      ethInflowUSDT shouldEqual expectedEthInflowInUsdt +- 0.0000001
    }
  }

}
