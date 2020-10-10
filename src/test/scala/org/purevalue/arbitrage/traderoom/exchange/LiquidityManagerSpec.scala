package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.{Balance, ExchangePublicDataReadonly, Ticker, Wallet}
import org.purevalue.arbitrage.traderoom.Asset.{AssetUSDT, Bitcoin, Euro, USDollar}
import org.purevalue.arbitrage.traderoom.TradeRoom.{FinishedLiquidityTx, LiquidityTransformationOrder, OrderRef}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLock, LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.{Asset, CryptoValue, TradePair, TradeSide}
import org.purevalue.arbitrage._
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
    with BeforeAndAfterAll {

  private val liquidityManagerConfig = LiquidityManagerConfig(
    Duration.ofSeconds(10),
    Duration.ofSeconds(3),
    maxAcceptableExchangeRateLossVersusReferenceTicker = 0.01,
    minimumKeepReserveLiquidityPerAssetInUSD = 50.0,
    txLimitAwayFromEdgeLimit = 0.00005,
    txValueGranularityInUSD = 20.0,
    dustLevelInUSD = 0.05)

  Asset.register("ETH", Some("Ethereum"), Some(false))
  Asset.register("ALGO", Some("Algorand"), Some(false))
  Asset.register("ADA", Some("Cardano"), Some(false))
  Asset.register("LINK", Some("Chainlink"), Some(false))
  Asset.register("OMG", None, Some(false))

  private val tradeRoomConfig = TradeRoomConfig(
    tradeSimulation = false, "e1", Duration.ofSeconds(1), Duration.ofSeconds(10), 20.0, Duration.ofSeconds(10), Duration.ofMinutes(1), null, List("e1")
  )

  private lazy val exchangeConfig: ExchangeConfig =
    ExchangeConfig(
      name = "e1",
      reserveAssets = List(AssetUSDT, Bitcoin, Asset("ETH")),
      assetBlocklist = Set(),
      feeRate = 0.0, // TODO make everything working including fees
      usdEquivalentCoin = AssetUSDT,
      doNotTouchTheseAssets = Seq(Asset("OMG")),
      secrets = SecretsConfig("", "", None),
      refCode = None,
      assetSourceWeight = 1,
      tickerIsRealtime = true
    )

  private val globalConfig = GlobalConfig(10.seconds, Timeout(4.seconds), Timeout(10.seconds))

  private lazy val config: Config = Config(globalConfig, tradeRoomConfig, Map("e1" -> exchangeConfig), liquidityManagerConfig)

  private val BitcoinPriceUSD = 10200.24
  private val EthPriceUSD = 342.12
  private lazy val referenceTicker =
    Map[TradePair, Double](
      TradePair(Bitcoin, AssetUSDT) -> BitcoinPriceUSD,
      TradePair(Asset("ETH"), AssetUSDT) -> EthPriceUSD,
      TradePair(Asset("ETH"), Bitcoin) -> EthPriceUSD / BitcoinPriceUSD,
      TradePair(Asset("ALGO"), AssetUSDT) -> 0.35,
      TradePair(Asset("ALGO"), Bitcoin) -> 0.35 / BitcoinPriceUSD,
      TradePair(Asset("ADA"), AssetUSDT) -> 0.0891,
      TradePair(Asset("ADA"), Bitcoin) -> 0.0891 / BitcoinPriceUSD,
      TradePair(Asset("LINK"), AssetUSDT) -> 10.55,
      TradePair(Asset("LINK"), Bitcoin) -> 10.55 / BitcoinPriceUSD,
      TradePair(Asset("LINK"), Asset("ETH")) -> 10.55 / EthPriceUSD,
      TradePair(Asset("OMG"), AssetUSDT) -> 3.50,
      TradePair(Asset("OMG"), Bitcoin) -> 3.50 / BitcoinPriceUSD
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

    def alignToTxGranularity(amount: Double, asset: Asset): Double = {
      val granularity = CryptoValue(AssetUSDT, liquidityManagerConfig.txValueGranularityInUSD).convertTo(asset, referenceTicker).amount
      (amount / granularity).ceil * granularity
    }

    "rebalance reserve assets" in {
      val tradeRoom = TestProbe()
      // @formatter:off
      val wallet: Wallet = Wallet("e1", Map(
        Bitcoin   -> Balance(Bitcoin, 1.0, 0.0),
        AssetUSDT -> Balance(AssetUSDT, 0.05, 0.0),
        Euro      -> Balance(Euro, 999.0, 0.0),
        USDollar  -> Balance(USDollar, 999, 0.0)
      ), exchangeConfig)
      // @formatter:on

      val m: ActorRef = system.actorOf(LiquidityManager.props(config, exchangeConfig, tradePairs, tpData, wallet, tradeRoom.ref,
        _ => None,
        _ => Some(FinishedLiquidityTx(null, null, null, null)), // liquidity balancer waits to see it's issued order to finish - here we just say YES
        () => referenceTicker))

      m ! LiquidityManager.HouseKeeping() // trigger housekeeping

      // expect:
      // BTC -> ETH [fill-up 50 USDT]
      // BTC (all but reserve minimum) -> USDT

      var messages: List[LiquidityTransformationOrder] = Nil
      messages = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder]) :: messages
      tradeRoom.reply(Some(OrderRef(messages.head.orderRequest.exchange, messages.head.orderRequest.tradePair, "foo")))

      messages = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder]) :: messages
      tradeRoom.reply(Some(OrderRef(messages.head.orderRequest.exchange, messages.head.orderRequest.tradePair, "foo")))

      println(messages)
      messages should have length 2
      val tpEthBtc = TradePair(Asset("ETH"), Bitcoin)
      val tpBtcUsdt = TradePair(Bitcoin, AssetUSDT)
      assert(messages.map(_.orderRequest.tradePair).toSet == Set(tpEthBtc, tpBtcUsdt))

      val orderEthBtc = messages.find(_.orderRequest.tradePair == tpEthBtc).get.orderRequest
      orderEthBtc.tradeSide shouldBe TradeSide.Buy
      val ethInflowUSDT = orderEthBtc.calcIncomingLiquidity.convertTo(AssetUSDT, tickers).amount
      val expectedEthInflowInUsdt = (liquidityManagerConfig.minimumKeepReserveLiquidityPerAssetInUSD /
        liquidityManagerConfig.txValueGranularityInUSD).ceil * liquidityManagerConfig.txValueGranularityInUSD

      ethInflowUSDT shouldEqual expectedEthInflowInUsdt +- CheckSpread

      val orderBtcUsdt = messages.find(_.orderRequest.tradePair == tpBtcUsdt).get.orderRequest
      orderBtcUsdt.tradeSide shouldBe TradeSide.Sell

      orderEthBtc.calcOutgoingLiquidity.asset shouldBe Bitcoin
      val alreadyReducedBTCAmountUSDT = orderEthBtc.calcOutgoingLiquidity.amount * BitcoinPriceUSD
      val expectedBtcOutflowUSDT: Double =
        ((1.0 * BitcoinPriceUSD - alreadyReducedBTCAmountUSDT - liquidityManagerConfig.minimumKeepReserveLiquidityPerAssetInUSD) /
          liquidityManagerConfig.txValueGranularityInUSD).floor * liquidityManagerConfig.txValueGranularityInUSD

      orderBtcUsdt.calcOutgoingLiquidity.asset shouldBe Bitcoin
      (orderBtcUsdt.calcOutgoingLiquidity.amount * BitcoinPriceUSD) shouldEqual alignToTxGranularity(expectedBtcOutflowUSDT, Bitcoin) +- 1.0 // diff is around 0.5 here, must be the exchange rate

      tradeRoom.expectNoMessage(1.second)
    }


    "provide demanded coins and transfer back not demanded liquidity" in {
      val tradeRoom = TestProbe()
      val wallet: Wallet = Wallet("e1",
        Map(
          Bitcoin -> adapter.Balance(Bitcoin, 1.0, 0.0),
          AssetUSDT -> adapter.Balance(AssetUSDT, 7.0, 0.0),
          Asset("ETH") -> adapter.Balance(Asset("ETH"), 20.0, 0.0),
          Asset("ADA") -> adapter.Balance(Asset("ADA"), 100.0, 0.0),
          Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 500.0, 0.0),
          Asset("OMG") -> adapter.Balance(Asset("OMG"), 1000.0, 0.0) // staked (in do-not-touch list)
        ), exchangeConfig)

      val m: ActorRef = system.actorOf(LiquidityManager.props(config, exchangeConfig, tradePairs, tpData, wallet, tradeRoom.ref,
        _ => None,
        _ => Some(FinishedLiquidityTx(null, null, null, null)), // liquidity balancer waits to see it's issued order to finish - here we just say YES
        () => referenceTicker))

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
      var messages: List[LiquidityTransformationOrder] = Nil
      for (_ <- 1 to 4) {
        messages = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder]) :: messages
        tradeRoom.reply(Some(OrderRef(messages.head.orderRequest.exchange, messages.head.orderRequest.tradePair, "foo")))
      }

      println(s"got expected 4 messages: \n${messages.mkString("\n")}")

      messages should have length 4
      val algoToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair(Asset("ALGO"), AssetUSDT)).map(_.orderRequest)
      val ethToLinkOrder = messages.find(_.orderRequest.tradePair == TradePair(Asset("LINK"), Asset("ETH"))).map(_.orderRequest)
      val ethToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair(Asset("ETH"), AssetUSDT)).map(_.orderRequest)
      val btcToUsdtOrder = messages.find(_.orderRequest.tradePair == TradePair(Bitcoin, AssetUSDT)).map(_.orderRequest)

      assert(algoToUsdtOrder.isDefined)
      algoToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      algoToUsdtOrder.get.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      algoToUsdtOrder.get.calcOutgoingLiquidity.amount shouldBe alignToTxGranularity(500.0, Asset("ALGO")) +- CheckSpread

      assert(ethToLinkOrder.isDefined)
      ethToLinkOrder.get.tradeSide shouldBe TradeSide.Buy
      ethToLinkOrder.get.calcIncomingLiquidity.asset shouldBe Asset("LINK")
      ethToLinkOrder.get.calcIncomingLiquidity.amount shouldBe
        alignToTxGranularity(25.0, Asset("LINK")) +- CheckSpread

      assert(ethToUsdtOrder.isDefined)
      ethToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      ethToUsdtOrder.get.calcOutgoingLiquidity.asset shouldBe Asset("ETH")
      ethToUsdtOrder.get.calcIncomingLiquidity.amount shouldBe
        alignToTxGranularity((
          (20.0 * EthPriceUSD - 50.0) / liquidityManagerConfig.txValueGranularityInUSD).floor *
          liquidityManagerConfig.txValueGranularityInUSD,
          AssetUSDT) +- CheckSpread

      assert(btcToUsdtOrder.isDefined)
      btcToUsdtOrder.get.tradeSide shouldBe TradeSide.Sell
      btcToUsdtOrder.get.calcIncomingLiquidity.asset shouldBe AssetUSDT
      btcToUsdtOrder.get.calcIncomingLiquidity.amount shouldBe
        alignToTxGranularity(
          ((1.0 * BitcoinPriceUSD - 50.0) / liquidityManagerConfig.txValueGranularityInUSD).floor * liquidityManagerConfig.txValueGranularityInUSD,
          AssetUSDT) +- CheckSpread

      tradeRoom.expectNoMessage(1.second)
    }


    "liquidity lock works and remains respected until cleared" in {
      val tradeRoom = TestProbe()
      val wallet = Wallet("e1", Map(
        Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 1000.0, 0.0),
        Euro -> adapter.Balance(Euro, 1000.0, 0.0)
      ), exchangeConfig)

      val m = system.actorOf(LiquidityManager.props(config, exchangeConfig, tradePairs, tpData, wallet, tradeRoom.ref,
        _ => None,
        _ => Some(FinishedLiquidityTx(null, null, null, null)), // liquidity balancer waits to see it's issued order to finish - here we just say YES
        () => referenceTicker))

      implicit val timeout: Timeout = 1.second
      val lock = Await.result(
        (m ? LiquidityRequest(
          UUID.randomUUID(),
          Instant.now,
          "e1",
          "foo",
          Seq(CryptoValue(Asset("ALGO"), 400.0)),
          Set(Bitcoin)
        )).mapTo[Option[LiquidityLock]],
        2.seconds
      )

      lock.isDefined shouldBe true
      lock.get.coins shouldBe Seq(CryptoValue(Asset("ALGO"), 400.0))

      // 400 ALGOs are locked, 400 others will remain as open demand, so 200 are "unused"
      val message1: LiquidityTransformationOrder = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder])

      val oRequest1 = message1.orderRequest
      message1.orderRequest.tradePair shouldBe TradePair(Asset("ALGO"), AssetUSDT)
      message1.orderRequest.tradeSide shouldBe TradeSide.Sell
      message1.orderRequest.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      val expectedRequest1OutgoingAlgos = alignToTxGranularity(200.0, Asset("ALGO"))
      message1.orderRequest.calcOutgoingLiquidity.amount shouldBe expectedRequest1OutgoingAlgos +- CheckSpread
      message1.orderRequest.calcIncomingLiquidity.asset shouldBe AssetUSDT
      message1.orderRequest.calcIncomingLiquidity.amount shouldBe
        (expectedRequest1OutgoingAlgos * tickers("e1")(TradePair(Asset("ALGO"), AssetUSDT)).priceEstimate * (1.0 - exchangeConfig.feeRate)) +- CheckSpread

      // traderoom replies liquidty tx order request
      tradeRoom.reply(Some(OrderRef("e1", oRequest1.tradePair, UUID.randomUUID().toString)))

      tradeRoom.expectNoMessage(1.seconds)

      // now we clear the lock and watch the locked 400 ALGO's going back to USDT

      // Manual Wallet update
      wallet.balance = wallet.balance + (Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 800.0, 0.0))
      m ! LiquidityLockClearance(lock.get.liquidityRequestId)
      val message2: LiquidityTransformationOrder = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder])

      val oRequest2 = message2.orderRequest
      message2.orderRequest.tradePair shouldBe TradePair(Asset("ALGO"), AssetUSDT)
      message2.orderRequest.tradeSide shouldBe TradeSide.Sell
      message2.orderRequest.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      message2.orderRequest.calcOutgoingLiquidity.amount shouldBe alignToTxGranularity(400.0, Asset("ALGO")) +- CheckSpread

      // traderoom replies liquidty tx order request
      tradeRoom.reply(Some(OrderRef("e1", oRequest2.tradePair, UUID.randomUUID().toString)))

      tradeRoom.expectNoMessage(1.seconds)

      // Manual Wallet update
      wallet.balance = wallet.balance + (Asset("ALGO") -> adapter.Balance(Asset("ALGO"), 400.0, 0.0))

      // remaining 400 ALGO demand should go back to USDT also after liquidityDemandActiveTime + houseKeeping
      Thread.sleep(liquidityManagerConfig.liquidityDemandActiveTime.toMillis + 500)
      m ! LiquidityManager.HouseKeeping() // trigger houseKeeping

      val message3: LiquidityTransformationOrder = tradeRoom.expectMsgClass(2.seconds, classOf[LiquidityTransformationOrder])

      val oRequest3 = message3.orderRequest
      message3.orderRequest.tradePair shouldBe TradePair(Asset("ALGO"), AssetUSDT)
      message3.orderRequest.tradeSide shouldBe TradeSide.Sell
      message3.orderRequest.calcOutgoingLiquidity.asset shouldBe Asset("ALGO")
      message3.orderRequest.calcOutgoingLiquidity.amount shouldBe alignToTxGranularity(400.0, Asset("ALGO")) +- CheckSpread

      // traderoom replies liquidty tx order request
      tradeRoom.reply(Some(OrderRef("e1", oRequest3.tradePair, UUID.randomUUID().toString)))

      tradeRoom.expectNoMessage(1.second)
    }
  }
}
