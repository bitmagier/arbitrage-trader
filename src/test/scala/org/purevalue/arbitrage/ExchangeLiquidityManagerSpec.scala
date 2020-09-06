package org.purevalue.arbitrage

import java.time.Duration

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.purevalue.arbitrage.Asset.{Bitcoin, USDT}
import org.purevalue.arbitrage.ExchangeLiquidityManager.LiquidityDemand
import org.purevalue.arbitrage.TradeRoom.{LiquidityTransformationOrder, ReferenceTicker, ReferenceTickerReadonly, WalletUpdateTrigger}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class ExchangeLiquidityManagerSpec
  extends TestKit(ActorSystem("ExchangeLiquidityManagerSpec"))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory {

  private val Config = LiquidityManagerConfig(List(USDT, Bitcoin, Asset("ETH")),
    Duration.ofSeconds(5),
    Duration.ofSeconds(5),
    providingLiquidityExtra = 0.02,
    maxAcceptableLocalTickerLossFromReferenceTicker = 0.01,
    minimumKeepReserveLiquidityPerAssetInUSDT = 50.0,
    txLimitBelowOrAboveBestBidOrAsk = 0.005,
    rebalanceTxGranularityInUSDT = 20.0)


  private val fee = Fee("e1", 0.002, 0.002)
  private val referenceTicker = ReferenceTickerReadonly(
    Map[TradePair, Double](
      TradePair.of(Bitcoin, USDT) -> 10000.0,
      TradePair.of(Asset("ETH"), USDT) -> 350.0,
      TradePair.of(Asset("ETH"), Bitcoin) -> 0.03316,
      TradePair.of(Asset("ALGO"), USDT) -> 0.35,
      TradePair.of(Asset("ALGO"), Bitcoin) -> 0.089422,
      TradePair.of(Asset("ADA"), USDT) -> 0.0891,
      TradePair.of(Asset("ADA"), Bitcoin) -> 0.00000875,
      TradePair.of(Asset("LINK"), USDT) -> 10.55,
      TradePair.of(Asset("LINK"), Bitcoin) -> 0.001063,
      TradePair.of(Asset("LINK"), Asset("ETH")) -> 0.0301,
    ).map(e => e._1 -> ExtendedTicker("e1", e._1, e._2, 1.0, e._2, 1.0, e._2, 1.0, e._2)))


  private val tickers: Map[TradePair, Ticker] = referenceTicker.values.map(e =>
    (e._1, Ticker("e1", e._1, e._2.highestBidPrice, None, e._2.lowestAskPrice, None, Some(e._2.lastPrice)))
  ).toMap

  private val tpData = ExchangeTPDataReadonly(tickers, Map(), Map(), null)

  val CheckSpread = 0.0000001

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
      val m = system.actorOf(ExchangeLiquidityManager.props(Config, "e1", tradeRoom.ref, tpData, wallet, fee, referenceTicker))

      m ! WalletUpdateTrigger("e1") // trigger housekeeping

      val messages: Seq[LiquidityTransformationOrder] = tradeRoom.expectMsgAllClassOf(2.seconds, classOf[LiquidityTransformationOrder], classOf[LiquidityTransformationOrder])
      println(messages)
      messages should have length 2
      val tpEthBtc = TradePair.of(Asset("ETH"), Bitcoin)
      val tpBtcUsdt = TradePair.of(Bitcoin, USDT)
      assert(messages.map(_.orderRequest.tradePair) == Seq(tpEthBtc, tpBtcUsdt))

      val orderBtcUsdt = messages.find(_.orderRequest.tradePair == tpBtcUsdt).get.orderRequest
      orderBtcUsdt.tradeSide shouldBe TradeSide.Sell
      orderBtcUsdt.calcIncomingLiquidity.asset shouldBe USDT
      val expectedUsdtInflow: Double = (Config.minimumKeepReserveLiquidityPerAssetInUSDT / Config.rebalanceTxGranularityInUSDT).ceil * Config.rebalanceTxGranularityInUSDT
      orderBtcUsdt.calcIncomingLiquidity.amount shouldEqual expectedUsdtInflow +- CheckSpread

      val orderEthBtc = messages.find(_.orderRequest.tradePair == tpEthBtc).get.orderRequest
      orderEthBtc.tradeSide shouldBe TradeSide.Buy
      val ethInflowUSDT = orderEthBtc.calcIncomingLiquidity.convertTo(USDT, tickers).map(_.amount).get
      val expectedEthInflowInUsdt = (Config.minimumKeepReserveLiquidityPerAssetInUSDT / Config.rebalanceTxGranularityInUSDT).ceil * Config.rebalanceTxGranularityInUSDT
      ethInflowUSDT shouldEqual expectedEthInflowInUsdt +- CheckSpread
    }

    "provide demanded coins and transfer back not demanded liquidity" in {
      val tradeRoom = TestProbe()
      val wallet: Wallet = Wallet(Map(
        Bitcoin -> Balance(Bitcoin, 1.0, 0.0),
        USDT -> Balance(USDT, 7.0, 0.0),
        Asset("ETH") -> Balance(Asset("ETH"), 20.0, 0.0),
        Asset("ADA") -> Balance(Asset("ADA"), 100.0, 0.0),
        Asset("ALGO") -> Balance(Asset("ALGO"), 500.0, 0.0)
      ))
      val m = system.actorOf(ExchangeLiquidityManager.props(Config, "e1", tradeRoom.ref, tpData, wallet, fee, referenceTicker))

      val liquidityDemand = Seq(CryptoValue(Asset("ADA"), 100.0), CryptoValue(Asset("LINK"), 25.0))
      m ! LiquidityDemand("e1", "foo", liquidityDemand, Set(Bitcoin))

      // expected:
      // 500.0 ALGO -> USDT (convert back to reserve asset)
      // BTC -> 25.0 LINK (providing liquidity demand)
      // ETH -> floor((20.0 * 350 - 50.0)/20)*20  USDT (reserve asset rebalance)
      // BTC -> floor((10000.0 - 50.0)/20)*20 USDT (reserve asset rebalance)
      val messages: Seq[LiquidityTransformationOrder] = tradeRoom.expectMsgAllClassOf(2.seconds,
        classOf[LiquidityTransformationOrder],
        classOf[LiquidityTransformationOrder],
        classOf[LiquidityTransformationOrder],
        classOf[LiquidityTransformationOrder])

      tradeRoom.expectNoMessage(1.second)

      println(s"got expected 4 messages: \n${messages.mkString("\n")}")

      messages should have length 4
      val algoToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair.of(Asset("ALGO"), USDT)).map(_.orderRequest)
      val ethToLinkOrder = messages.find(_.orderRequest.tradePair == TradePair.of(Asset("LINK"), Asset("ETH"))).map(_.orderRequest)
      val ethToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair.of(Asset("ETH"), USDT)).map(_.orderRequest)
      val btcToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair.of(Bitcoin, USDT)).map(_.orderRequest)

      assert(algoToUsdtOrder.isDefined)
      algoToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      algoToUsdtOrder.get.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      algoToUsdtOrder.get.calcOutgoingLiquidity.amount shouldBe 500.0 +- CheckSpread

      assert(ethToLinkOrder.isDefined)
      ethToLinkOrder.get.tradeSide shouldBe TradeSide.Buy
      ethToLinkOrder.get.calcIncomingLiquidity.asset shouldBe Asset("LINK")
      ethToLinkOrder.get.calcIncomingLiquidity.amount shouldBe (25.0 * (1.0 + Config.providingLiquidityExtra)) +- CheckSpread

      assert(ethToUsdtOrder.isDefined)
      ethToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      ethToUsdtOrder.get.calcOutgoingLiquidity.asset shouldBe Asset("ETH")
      ethToUsdtOrder.get.calcIncomingLiquidity.amount shouldBe
        ((20.0 * 350 - 50.0) / Config.rebalanceTxGranularityInUSDT).floor * Config.rebalanceTxGranularityInUSDT +- CheckSpread

      assert(btcToUsdtOrder.isDefined)
      btcToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      btcToUsdtOrder.get.calcIncomingLiquidity.asset shouldBe USDT
      btcToUsdtOrder.get.calcIncomingLiquidity.amount shouldBe
        ((10000.0 - 50.0) / Config.rebalanceTxGranularityInUSDT).floor * Config.rebalanceTxGranularityInUSDT +- CheckSpread
    }
  }
}
