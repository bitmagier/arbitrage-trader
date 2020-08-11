package org.purevalue.arbitrage

import java.time.{Duration, LocalDateTime}
import java.util.UUID

import akka.actor.{Actor, Props}
import org.purevalue.arbitrage.ExchangeLiquidityManager.LiquidityRequest

/*
- LiquidityManager responsible for providing the Assets which are demanded by Traders.
     There is one global manager, which:
        - requests withdrawal jobs to balance liquidity among exchanges
     and one manager per exchange, which:
        - manages liquidity storing assets (like BTC, USDT) (currently unused altcoin liquidity goes back to these)
        - provides urgent requests for liquidity demands of assets (non liquitidy storing assets) required for upcoming trade requests

   [Concept]
   - Every single valid TradeRequest (no matter if enough balance is available or not) will result in a Liquidity-Request,
     which may be granted or not, based on the available (yet unreserved) asset balance
   - If that Liquidity-Request is covered by the current balance of the corresponding wallet,
       then it is Granted and this amount in the AssetWallet is marked as reserved for a limited duration (Asset-Amount-Reserve)
   - Else, if that Liquidity-Request is not covered by the current balance, then
       - it is Rejected and a LiquidityDemand is noticed by the ExchangeLiquidityManager,
         which may result in a Liquidity-Trade in favor of the requested Asset balance, in case enough Reserve-Liquidity is available.
    - [Concept Liquidity-Trade-Request]
       - The Liquidity-Trade can only be fulfilled, when enough amount of one of the configured Reserve-Assets is available
       - Furthermore it can only be fulfilled by a Reserve-Asset, which is not equal to the intendedByAsset of the Liquidity-Request
       - Furthermore it can only be fulfilled, if the current exchange-rate on this exchange is good enough,
         which means, it must be close to the Reference-Ticker exchange-Rate or better than that (if we get even more demand-amount per reserve-asset-coin)

   - Every completed trade (no matter if succeded or cancelled) will result in a Clearance of it's previously sent Liquidity-Request,
     Clearance of a Liquidity-Request means:
     - removal of the corresponding Asset-Amount-Reserve
   - In case, that the maximum lifetime of a liquidity-request is reached, it will be cleared automatically

   [Concept Reserve-Liquidity-Management]
   - Unused (not reserved) liquidity of a non-Reserve-Asset will be automatically converted to a Reserve-Asset.
     Which reserve-asset it will be, is determined by:
     - [non-loss-asset-filter] Filtering acceptable ticker value on that exchange compared to reference-ticker-value
     - [fill-up] Try to reach minimum configured balance of each reserve-assets in their order of preference
     - [play safe] Remaining value goes to first (highest prio) reserve-asset
*/

object ExchangeLiquidityManager {

  case class LiquidityRequest(id:UUID, asset:Asset, amount:Double, intendedBuyAsset:Asset)

  def props(config:LiquidityManagerConfig, exchangeConfig: ExchangeConfig, balances: Map[Asset, AssetWallet]): Props =
    Props(new ExchangeLiquidityManager(config, exchangeConfig, balances))
}
class ExchangeLiquidityManager(config:LiquidityManagerConfig,
                               exchangeConfig: ExchangeConfig,
                               balances: Map[Asset, AssetWallet]) extends Actor {

  case class RunningDemand(asset:Asset, amount:Double, requestTime:LocalDateTime)

  val liquidityRequestLifetime: Duration = Duration.ofSeconds(10) // TODO config


  override def receive: Receive = {
    case r:LiquidityRequest =>
  }
}
