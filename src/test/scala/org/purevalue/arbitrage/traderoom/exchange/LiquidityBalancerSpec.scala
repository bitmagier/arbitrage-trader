package org.purevalue.arbitrage.traderoom.exchange

import java.time.Duration

import org.purevalue.arbitrage.traderoom.Asset.{BTC, USDT}
import org.purevalue.arbitrage.traderoom.TradeSide.{Buy, Sell}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.{LiquidityTransfer, WorkingContext}
import org.purevalue.arbitrage.traderoom.{Asset, CryptoValue, TradePair, TradeSide}
import org.purevalue.arbitrage.{Config, ExchangeConfig, GlobalConfig, LiquidityManagerConfig, TradeRoomConfig}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class LiquidityBalancerSpec extends AnyWordSpecLike
  with Matchers {

  Asset.register("ETH", "Ethereum", isFiat = false)
  Asset.register("ALGO", "Algorand", isFiat = false)
  Asset.register("ADA", "Cardano", isFiat = false)
  Asset.register("LINK", "Link", isFiat = false)
  Asset.register("OMG", "OMG", isFiat = false)

  val ETH: Asset = Asset("ETH")
  val ALGO: Asset = Asset("ALGO")
  val ADA: Asset = Asset("ADA")
  val LINK: Asset = Asset("LINK")
  val OMG: Asset = Asset("OMG")

  val ExchangeName = "foo"
  val TxGranularityUSD = 20.0
  val Cfg: Config = org.purevalue.arbitrage.Config(
    GlobalConfig(1.second, 1.second, 1.second),
    TradeRoomConfig(tradeSimulation = true, ExchangeName, null, null, 50.0, null, null, null, List(ExchangeName)),
    Map(ExchangeName -> ExchangeConfig(ExchangeName, deliversOrderBook = true, deliversStats24h = false, List(USDT, BTC, ETH), Set(), USDT,
      0.0, Set(), tickerIsRealtime = true, null, None, 1, Duration.ZERO, 1.0)),

    LiquidityManagerConfig(null, null, 0.0002, 55.0, 1.0, 0.0001, TxGranularityUSD))

  private val BitcoinPriceUSD = 10200.24
  private val EthPriceUSD = 342.12
  private lazy val ExchangeTicker: Map[TradePair, Ticker] =
    Map[TradePair, Double](
      TradePair(BTC, USDT) -> BitcoinPriceUSD,
      TradePair(ETH, USDT) -> EthPriceUSD,
      TradePair(ETH, BTC) -> EthPriceUSD / BitcoinPriceUSD,
      TradePair(ALGO, USDT) -> 0.35,
      TradePair(ALGO, BTC) -> 0.35 / BitcoinPriceUSD,
      TradePair(ADA, USDT) -> 0.0891,
      TradePair(ADA, BTC) -> 0.0891 / BitcoinPriceUSD,
      TradePair(LINK, USDT) -> 10.55,
      TradePair(LINK, BTC) -> 10.55 / BitcoinPriceUSD,
      TradePair(LINK, ETH) -> 10.55 / EthPriceUSD,
      TradePair(OMG, USDT) -> 3.50,
      TradePair(OMG, BTC) -> 3.50 / BitcoinPriceUSD
    ).map(e => e._1 -> Ticker(ExchangeName, e._1, e._2, None, e._2, None, Some(e._2)))

  val TradePairs: Set[TradePair] = ExchangeTicker.keySet
  val BalanceSnapshot: Map[Asset, Balance] = Map(
    BTC -> 1.0,
    LINK -> 500.0
  ).map(e => e._1 -> Balance(e._1, e._2, 0.0))

  val wc: WorkingContext = WorkingContext(ExchangeTicker, ExchangeTicker, Map(), BalanceSnapshot, Map(), Map())
  val defaultLiquidityBalancer: LiquidityBalancer = new LiquidityBalancer(Cfg, Cfg.exchanges(ExchangeName), TradePairs, wc)

  "LiquidityBalancer" must {
    "calc buckets correctly" in {
      defaultLiquidityBalancer.bucketSize(ALGO) shouldBe 20.0 / 0.35 +- 0.00001

      defaultLiquidityBalancer.tradePairAndSide(ALGO, BTC) shouldBe(TradePair(ALGO, BTC), Sell)

      defaultLiquidityBalancer.calcRemainingSupply(
        Map(BTC -> 100, ETH -> 50),
        Seq(LiquidityTransfer(TradePair(ALGO, BTC), Buy, 2, 2 * 20.0 / 0.35, 0.35, 0.0))
      ) shouldBe Map(BTC -> 98, ETH -> 50)

      defaultLiquidityBalancer.toBucketsRound(BTC, 1.0) shouldBe ((1.0 * BitcoinPriceUSD) / Cfg.liquidityManager.txValueGranularityInUSD).round
      defaultLiquidityBalancer.toSupplyBuckets(BTC, 1.0) shouldBe ((1.0 * BitcoinPriceUSD) / Cfg.liquidityManager.txValueGranularityInUSD).round - 1

      defaultLiquidityBalancer.calcPureSupplyOverhead shouldBe Map(
        BTC -> defaultLiquidityBalancer.toSupplyBuckets(BTC, 1.0),
        LINK -> defaultLiquidityBalancer.toSupplyBuckets(LINK, 500.0)
      )
    }

    "determine needed transfers" in {
      val orders = defaultLiquidityBalancer.calculateOrders()

      LiquidityBalancerStats.logStats()

      orders should have size 3
      val linkToEthOrder = orders.find(e =>
        e.exchange == ExchangeName
          && e.pair == TradePair(LINK, ETH)
          && e.side == Sell)
      val btcToUsdtOrder = orders.find(e =>
        e.exchange == ExchangeName
          && e.pair == TradePair(BTC, USDT)
          && e.side == Sell)
      val linkToUsdtOrder = orders.find(e =>
        e.exchange == ExchangeName
          && e.pair == TradePair(LINK, USDT)
          && e.side == Sell)

      linkToEthOrder.isDefined shouldBe true
      btcToUsdtOrder.isDefined shouldBe true
      linkToUsdtOrder.isDefined shouldBe true

      linkToEthOrder.get.amountBaseAsset shouldBe (3 * 20.0) / 10.55 +- 0.000001
      linkToEthOrder.get.limit shouldBe ((10.55 / EthPriceUSD) * (1.0 - Cfg.liquidityManager.tickerBasedTxLimitBeyondEdgeLimit)) +- 0.00001

      {
        val btcBucketSize = defaultLiquidityBalancer.bucketSize(BTC)
        val minLeftBtcSupply = CryptoValue(USDT, Cfg.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD)
          .convertTo(BTC, wc.ticker).amount
        val maxTransferValue = 1.0 - minLeftBtcSupply
        val minTransferValue = ((maxTransferValue / btcBucketSize).round - 1) * btcBucketSize
        println(s"BTC ${btcToUsdtOrder.get.amountBaseAsset} between $minTransferValue and $maxTransferValue}")
        btcToUsdtOrder.get.amountBaseAsset should (be >= minTransferValue and be <= maxTransferValue)
      }

      {
        val linkBucketSize = defaultLiquidityBalancer.bucketSize(LINK)
        val maxTransferValue = 500.0 - linkToEthOrder.get.amountBaseAsset
        val minTransferValue = ((maxTransferValue / linkBucketSize).round - 1) * linkBucketSize
        println(s"LINK ${linkToUsdtOrder.get.amountBaseAsset} between $minTransferValue and $maxTransferValue}")
        linkToUsdtOrder.get.amountBaseAsset should (be >= minTransferValue and be <= maxTransferValue)
      }
    }

    "squash transfers" in {
      val t1 = Seq(
        LiquidityTransfer(TradePair(ETH, BTC), TradeSide.Buy, 2, 100.0, 0.1, 0.0),
        LiquidityTransfer(TradePair(ETH, BTC), TradeSide.Buy, 2, 100.0, 0.1, 0.0),
        LiquidityTransfer(TradePair(ETH, USDT), TradeSide.Sell, 1, 100.0, 200, 0.0),
      )
      val t2 = Seq(LiquidityTransfer(TradePair(ETH, USDT), TradeSide.Sell, 2, 100.0, 200, 0.0))
      val squashed = defaultLiquidityBalancer.squash(t1 ++ t2)

      squashed should have size 2
      val r1 = squashed.find(e => e.pair == TradePair(ETH, BTC)).get
      val r2 = squashed.find(e => e.pair == TradePair(ETH, USDT)).get

      r1.side shouldBe TradeSide.Buy
      r1.buckets shouldBe 4
      r1.quantity shouldBe 200.0

      r2.side shouldBe TradeSide.Sell
      r2.buckets shouldBe 3
      r2.quantity shouldBe 200.0
    }
  }
}
