package org.purevalue.arbitrage.traderoom

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataChannel, BinancePublicDataInquirer}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexAccountDataChannel, BitfinexPublicDataChannel, BitfinexPublicDataInquirer}
import org.purevalue.arbitrage.adapter.coinbase.{CoinbaseAccountDataChannel, CoinbasePublicDataChannel, CoinbasePublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator.{AllTradePairs, Reply}
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.{Exchange, ExchangeAccountDataChannelInit, ExchangePublicDataChannelInit, ExchangePublicDataInquirerInit}
import org.purevalue.arbitrage.{Config, Main, UserRootGuardian}
import org.slf4j.LoggerFactory


case class ExchangeInitStuff(exchangePublicDataInquirerProps: ExchangePublicDataInquirerInit,
                             exchangePublicDataChannelProps: ExchangePublicDataChannelInit,
                             exchangeAccountDataChannelProps: ExchangeAccountDataChannelInit)

object TradeRoomInitCoordinator {
  def apply(config: Config,
            parent: ActorRef[UserRootGuardian.Reply]):
  Behavior[Reply] =
    Behaviors.setup(context => new TradeRoomInitCoordinator(context, config, parent))

  sealed trait Reply
  case class AllTradePairs(exchange: String, pairs: Set[TradePair]) extends Reply
  case class UsableTradePairsSet(exchange: String)
  case class StreamingStarted(exchange: String)
}
class TradeRoomInitCoordinator(context: ActorContext[TradeRoomInitCoordinator.Reply],
                               config: Config,
                               parent: ActorRef[UserRootGuardian.Reply])
  extends AbstractBehavior[TradeRoomInitCoordinator.Reply](context) {

  private val log = LoggerFactory.getLogger(getClass)
  implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem

  // @formatter:off
  var allTradePairs:      Map[String, Set[TradePair]] = Map()
  var exchanges:          Map[String, ActorRef[Exchange.Message]] = Map()
  // @formatter:on

  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      BinancePublicDataInquirer.apply,
      BinancePublicDataChannel.apply,
      BinanceAccountDataChannel.apply
    ),
    "bitfinex" -> ExchangeInitStuff(
      BitfinexPublicDataInquirer.apply,
      BitfinexPublicDataChannel.apply,
      BitfinexAccountDataChannel.apply
    ),
    "coinbase" -> ExchangeInitStuff(
      CoinbasePublicDataInquirer.apply,
      CoinbasePublicDataChannel.apply,
      CoinbaseAccountDataChannel.apply
    )
  )

  // we want to keep only trade pairs
  // - [1] which are NOT connected to Fiat money
  // - [2] that are available on at least two exchanges
  // - [3] plus trade pairs, where one side is a part of [2] and the other side is a local reserve asset
  // - [4] plus all crypto-coin to USD equivalent coin pairs (for liquidity calculations & conversions)
  def determineUsableTradepairs(): Map[String, Set[TradePair]] = {
    val allGlobalTradePairs = allTradePairs.values.flatten.toSet
    val globalArbitragePairs = allGlobalTradePairs
      .filterNot(e => e.involvedAssets.exists(_.isFiat))
      .filter(e => allTradePairs.count(_._2.contains(e)) > 1)

    val arbitrageAssets = globalArbitragePairs.flatMap(_.involvedAssets)

    def condition3(exchange: String, pair: TradePair): Boolean = {
      (arbitrageAssets.contains(pair.baseAsset) && (pair.quoteAsset == config.exchanges(exchange).usdEquivalentCoin || config.exchanges(exchange).reserveAssets.contains(pair.quoteAsset))) ||
        (arbitrageAssets.contains(pair.quoteAsset) && pair.baseAsset == config.exchanges(exchange).usdEquivalentCoin || config.exchanges(exchange).reserveAssets.contains(pair.baseAsset))
    }

    def condition4(exchange: String, pair: TradePair): Boolean = {
      !pair.baseAsset.isFiat && pair.quoteAsset == config.exchanges(exchange).usdEquivalentCoin
    }

    val usableTradePairs = allTradePairs
      .map(e => e._1 ->
        e._2.filter(x =>
          globalArbitragePairs.contains(x)
            || condition3(e._1, x)
            || condition4(e._1, x)
        ))

    for (exchange <- exchanges.keySet) {
      log.info(s"[$exchange] unusable trade pairs: ${(allTradePairs(exchange) -- usableTradePairs(exchange)).toSeq.sortBy(_.toString)}")
    }

    usableTradePairs
  }


  def startExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    exchanges = exchanges +
      (exchangeName -> context.spawn(
        Exchange(
          exchangeName,
          config,
          config.exchanges(exchangeName),
          exchangeInit
        ), "Exchange-" + exchangeName))
  }


  def initialized(): Behavior[Reply] = {
    log.debug("TradeRoom initialized")
    val tradeRoom = context.spawn(TradeRoom(config, exchanges), "TradeRoom")
    parent ! UserRootGuardian.TradeRoomInitialized(tradeRoom)
    context.watch(tradeRoom)
    Behaviors.empty
  }

  def sendStartStreaming(): Behavior[Reply] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    exchanges.foreach {
      case (name, actor) =>
        actor.ask(ref => StartStreaming(ref))
        log.debug(s"streaming started on $name")
    }
    initialized()
  }

  def pushUsableTradePairs(usableTradePairs: Map[String, Set[TradePair]]): Behavior[Reply] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    exchanges.foreach {
      case (exchange, actor) => actor.ask(ref => SetUsableTradePairs(usableTradePairs(exchange), ref))
    }
    sendStartStreaming()
  }

  def startExchanges(): Unit = {
    for (name: String <- config.exchanges.keys) {
      startExchange(name, AllExchanges(name))
    }
  }

  override def onMessage(msg: Reply): Behavior[Reply] = {
    msg match {
      case AllTradePairs(exchange, pairs) =>
        allTradePairs = allTradePairs + (exchange -> pairs)
        log.debug(s"received trade pairs for $exchange")

        if (allTradePairs.keySet != exchanges.keySet) {
          Behaviors.same
        } else {
          log.info("trade pairs received")

          pushUsableTradePairs(
            determineUsableTradepairs()
          )
        }
    }
  }

  startExchanges() // parallel
  exchanges.keys.foreach(exchange =>
    exchanges(exchange) ! GetAllTradePairs(context.self)
  )
}

// TODO
//  override val supervisorStrategy: AllForOneStrategy = {
//    AllForOneStrategy() {
//      case _: Exception => Escalate
//    }
