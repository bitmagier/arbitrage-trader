package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.{Balance, ExchangePublicDataReadonly, Ticker, Wallet}
import org.purevalue.arbitrage.traderoom.Asset.{Bitcoin, Euro, USDT, USDollar}
import org.purevalue.arbitrage.traderoom.TradeRoom.LiquidityTransformationOrder
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.{Asset, CryptoValue, TradePair, TradeSide}
import org.purevalue.arbitrage.{ExchangeConfig, LiquidityManagerConfig, adapter}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class LiquidityManagerSpec
  extends TestKit(ActorSystem("ExchangeLiquidityManagerSpec"))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
{
  private val liquidityManagerConfig = LiquidityManagerConfig(
      Duration.ofSeconds(5),
      Duration.ofSeconds(5),
      providingLiquidityExtra = 0.02,
      maxAcceptableExchangeRateLossVersusReferenceTicker = 0.01,
      minimumKeepReserveLiquidityPerAssetInUSDT = 50.0,
      txLimitBelowOrAboveBestBidOrAsk = 0.00005,
      rebalanceTxGranularityInUSDT = 20.0,
      dustLevelInUsdt = 0.05)

  private val exchangeConfig: ExchangeConfig =
    ExchangeConfig(
      "e1",
      secrets = null,
      reserveAssets = List(USDT, Bitcoin, Asset("ETH")),
      tradeAssets = null,
      makerFee = 0.0, // TODO make everything working including fees
      takerFee = 0.0,
      Seq(Asset("OMG")),
      None
    )

  private val BitcoinPriceUSD = 10200.24
  private val EthPriceUSD = 342.12
  private val referenceTicker =
    Map[TradePair, Double](
      TradePair(Bitcoin, USDT) -> BitcoinPriceUSD,
      TradePair(Asset("ETH"), USDT) -> EthPriceUSD,
      TradePair(Asset("ETH"), Bitcoin) -> EthPriceUSD/BitcoinPriceUSD,
      TradePair(Asset("ALGO"), USDT) -> 0.35,
      TradePair(Asset("ALGO"), Bitcoin) -> 0.35/BitcoinPriceUSD,
      TradePair(Asset("ADA"), USDT) -> 0.0891,
      TradePair(Asset("ADA"), Bitcoin) -> 0.0891/BitcoinPriceUSD,
      TradePair(Asset("LINK"), USDT) -> 10.55,
      TradePair(Asset("LINK"), Bitcoin) -> 10.55/BitcoinPriceUSD,
      TradePair(Asset("LINK"), Asset("ETH")) -> 0.0301,
      TradePair(Asset("OMG"), USDT) -> 3.50,
      TradePair(Asset("OMG"), Bitcoin) -> 3.50/BitcoinPriceUSD
    ).map(e => e._1 -> Ticker("e1", e._1, e._2, None, e._2, None, Some(e._2)))

  private val tradePairs: Set[TradePair] = referenceTicker.keySet


  private val tickers: Map[String, Map[TradePair, Ticker]] =
    Map("e1" -> referenceTicker)

  private val tpData = ExchangePublicDataReadonly(tickers("e1"), Map())

  val CheckSpread = 0.01

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "ExchangeLiquidityManager" must {

    "rebalance reserve assets" in {
      val tradeRoom = TestProbe()
      val wallet: Wallet = Wallet("e1", Map(
        Bitcoin -> Balance(Bitcoin, 1.0, 0.0),
        USDT -> adapter.Balance(USDT, 0.05, 0.0),
        Euro -> adapter.Balance(Euro, 999.0, 0.0),
        USDollar -> adapter.Balance(USDollar, 999, 0.0)
      ), exchangeConfig)

      val m: ActorRef = system.actorOf(LiquidityManager.props(liquidityManagerConfig, exchangeConfig, tradePairs, tpData, wallet, tradeRoom.ref, _ => None, () => referenceTicker))

      m ! LiquidityManager.HouseKeeping() // trigger housekeeping

      // expect:
      // BTC -> ETH [fill-up 50 USDT]
      // BTC (all but reserve minimum) -> USDT

      val messages: Seq[LiquidityTransformationOrder] = tradeRoom.expectMsgAllClassOf(2.seconds, classOf[LiquidityTransformationOrder], classOf[LiquidityTransformationOrder])
      tradeRoom.expectNoMessage(1.second)
      println(messages)
      messages should have length 2
      val tpEthBtc = TradePair(Asset("ETH"), Bitcoin)
      val tpBtcUsdt = TradePair(Bitcoin, USDT)
      assert(messages.map(_.orderRequest.tradePair).toSet == Set(tpEthBtc, tpBtcUsdt))

      val orderEthBtc = messages.find(_.orderRequest.tradePair == tpEthBtc).get.orderRequest
      orderEthBtc.tradeSide shouldBe TradeSide.Buy
      val ethInflowUSDT = orderEthBtc.calcIncomingLiquidity.convertTo(USDT, tickers).amount
      val expectedEthInflowInUsdt = (liquidityManagerConfig.minimumKeepReserveLiquidityPerAssetInUSDT / liquidityManagerConfig.rebalanceTxGranularityInUSDT).ceil * liquidityManagerConfig.rebalanceTxGranularityInUSDT
      ethInflowUSDT shouldEqual expectedEthInflowInUsdt +- CheckSpread


      val orderBtcUsdt = messages.find(_.orderRequest.tradePair == tpBtcUsdt).get.orderRequest
      orderBtcUsdt.tradeSide shouldBe TradeSide.Sell

      orderEthBtc.calcOutgoingLiquidity.asset shouldBe Bitcoin
      val alreadyReducedBTCAmountUSDT = orderEthBtc.calcOutgoingLiquidity.amount * BitcoinPriceUSD
      val expectedBtcOutflowUSDT: Double = ((1.0 * BitcoinPriceUSD - alreadyReducedBTCAmountUSDT - liquidityManagerConfig.minimumKeepReserveLiquidityPerAssetInUSDT) /
        liquidityManagerConfig.rebalanceTxGranularityInUSDT).floor * liquidityManagerConfig.rebalanceTxGranularityInUSDT
      orderBtcUsdt.calcOutgoingLiquidity.asset shouldBe Bitcoin
      (orderBtcUsdt.calcOutgoingLiquidity.amount * BitcoinPriceUSD) shouldEqual expectedBtcOutflowUSDT +- 1.0 // diff is around 0.5 here, must be the exchange rate
    }

    "provide demanded coins and transfer back not demanded liquidity" in {
      val tradeRoom = TestProbe()
      val wallet: Wallet = Wallet("e1", Map(
        Bitcoin -> adapter.Balance(Bitcoin, 1.0, 0.0),
        USDT -> adapter.Balance(USDT, 7.0, 0.0),
        Asset("ETH") -> adapter.Balance(Asset("ETH"), 20.0, 0.0),
        Asset("ADA") -> adapter.Balance(Asset("ADA"), 100.0, 0.0),
        Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 500.0, 0.0),
        Asset("OMG") -> adapter.Balance(Asset("OMG"), 1000.0, 0.0) // staked (in do-not-touch list)
      ), exchangeConfig)

      val m: ActorRef = system.actorOf(LiquidityManager.props(liquidityManagerConfig, exchangeConfig, tradePairs, tpData, wallet, tradeRoom.ref, _ => None, () => referenceTicker))

      val requestedLiquidity = Seq(CryptoValue(Asset("ADA"), 100.0), CryptoValue(Asset("LINK"), 25.0))
      implicit val timeout: Timeout = 2.seconds
      val lock = Await.result(
        (m ? LiquidityRequest(UUID.randomUUID(), Instant.now, "e1", "foo", requestedLiquidity, Set(Bitcoin))).mapTo[Option[LiquidityLock]],
        2.second)

      lock.isEmpty shouldBe true

      // expected:
      // 500.0 ALGO -> USDT (convert back to reserve asset)
      // BTC -> 25.0 LINK (providing liquidity demand)
      // ETH -> floor((20.0 * EthPriceUSD - 50.0)/20)*20  USDT (reserve asset rebalance)
      // BTC -> floor((1.0 * BitcoinPriceUSD - 50.0)/20)*20 USDT (reserve asset rebalance)
      val messages: Seq[LiquidityTransformationOrder] = tradeRoom.expectMsgAllClassOf(2.seconds,
        classOf[LiquidityTransformationOrder],
        classOf[LiquidityTransformationOrder],
        classOf[LiquidityTransformationOrder],
        classOf[LiquidityTransformationOrder])

      tradeRoom.expectNoMessage(1.second)

      println(s"got expected 4 messages: \n${messages.mkString("\n")}")

      messages should have length 4
      val algoToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair(Asset("ALGO"), USDT)).map(_.orderRequest)
      val ethToLinkOrder = messages.find(_.orderRequest.tradePair == TradePair(Asset("LINK"), Asset("ETH"))).map(_.orderRequest)
      val ethToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair(Asset("ETH"), USDT)).map(_.orderRequest)
      val btcToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair(Bitcoin, USDT)).map(_.orderRequest)

      assert(algoToUsdtOrder.isDefined)
      algoToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      algoToUsdtOrder.get.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      algoToUsdtOrder.get.calcOutgoingLiquidity.amount shouldBe 500.0 +- CheckSpread

      assert(ethToLinkOrder.isDefined)
      ethToLinkOrder.get.tradeSide shouldBe TradeSide.Buy
      ethToLinkOrder.get.calcIncomingLiquidity.asset shouldBe Asset("LINK")
      ethToLinkOrder.get.calcIncomingLiquidity.amount shouldBe (25.0 * (1.0 + liquidityManagerConfig.providingLiquidityExtra)) +- CheckSpread

      assert(ethToUsdtOrder.isDefined)
      ethToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      ethToUsdtOrder.get.calcOutgoingLiquidity.asset shouldBe Asset("ETH")
      ethToUsdtOrder.get.calcIncomingLiquidity.amount shouldBe
        ((20.0 * EthPriceUSD - 50.0) / liquidityManagerConfig.rebalanceTxGranularityInUSDT).floor * liquidityManagerConfig.rebalanceTxGranularityInUSDT +- CheckSpread

      assert(btcToUsdtOrder.isDefined)
      btcToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      btcToUsdtOrder.get.calcIncomingLiquidity.asset shouldBe USDT
      btcToUsdtOrder.get.calcIncomingLiquidity.amount shouldBe
        ((1.0 * BitcoinPriceUSD - 50.0) / liquidityManagerConfig.rebalanceTxGranularityInUSDT).floor * liquidityManagerConfig.rebalanceTxGranularityInUSDT +- CheckSpread
    }

    "liquidity lock works and remains respected until cleared" in {
      val tradeRoom = TestProbe()
      val wallet = Wallet("e1", Map(
        Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 1000.0, 0.0),
        Euro -> adapter.Balance(Euro, 1000.0, 0.0)
      ), exchangeConfig)

      val m = system.actorOf(LiquidityManager.props(liquidityManagerConfig, exchangeConfig, tradePairs, tpData, wallet, tradeRoom.ref, _ => None, () => referenceTicker))

      implicit val timeout: Timeout = 2.seconds
      val lock = Await.result(
        (m ? LiquidityRequest(
          UUID.randomUUID(),
          Instant.now,
          "e1",
          "foo",
          Seq(CryptoValue(Asset("ALGO"), 400.0)),
          Set(Bitcoin)
        )).mapTo[Option[LiquidityLock]],
        1.second
      )

      lock.isDefined shouldBe true
      lock.get.coins shouldBe Seq(CryptoValue(Asset("ALGO"), 400.0))

      // 400 ALGOs are locked, 400 others will remain as open demand, so 200 are "unused"

      val message1: LiquidityTransformationOrder = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder])
      tradeRoom.expectNoMessage(1.second)

      message1.orderRequest.tradePair shouldBe TradePair(Asset("ALGO"), USDT)
      message1.orderRequest.tradeSide shouldBe TradeSide.Sell
      message1.orderRequest.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      message1.orderRequest.calcOutgoingLiquidity.amount shouldBe 200.0 +- CheckSpread
      message1.orderRequest.calcIncomingLiquidity.asset shouldBe USDT
      message1.orderRequest.calcIncomingLiquidity.amount shouldBe
        (200.0 * tickers("e1")(TradePair(Asset("ALGO"), USDT)).priceEstimate * (1.0 - exchangeConfig.fee.average)) +- CheckSpread

      // now we clear the lock and watch the locked 400 ALOG's going back to USDT

      // Manual Wallet update
      wallet.balance = wallet.balance + (Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 800.0, 0.0))
      m ! LiquidityLockClearance(lock.get.liquidityRequestId)
      val message2: LiquidityTransformationOrder = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder])
      tradeRoom.expectNoMessage(1.second)

      message2.orderRequest.tradePair shouldBe TradePair(Asset("ALGO"), USDT)
      message2.orderRequest.tradeSide shouldBe TradeSide.Sell
      message2.orderRequest.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      message2.orderRequest.calcOutgoingLiquidity.amount shouldBe 400.0 +- CheckSpread

      // Manual Wallet update
      wallet.balance = wallet.balance + (Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 400.0, 0.0))

      // remaining 400 ALGO demand should go back to USDT also after liquidityDemandActiveTime + houseKeeping
      Thread.sleep(liquidityManagerConfig.liquidityDemandActiveTime.toMillis + 500)
      m ! LiquidityManager.HouseKeeping() // trigger houseKeeping
      Thread.sleep(500)

      val message3: LiquidityTransformationOrder = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder])
      tradeRoom.expectNoMessage(1.second)

      message3.orderRequest.tradePair shouldBe TradePair(Asset("ALGO"), USDT)
      message3.orderRequest.tradeSide shouldBe TradeSide.Sell
      message3.orderRequest.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      message3.orderRequest.calcOutgoingLiquidity.amount shouldBe 400.0 +- CheckSpread
    }
  }
}
