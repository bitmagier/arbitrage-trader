package org.purevalue.arbitrage.traderoom

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataChannel, BinancePublicDataInquirer}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexAccountDataChannel, BitfinexPublicDataChannel, BitfinexPublicDataInquirer}
import org.purevalue.arbitrage.adapter.coinbase.{CoinbaseAccountDataChannel, CoinbasePublicDataChannel, CoinbasePublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator.{AllTradePairs, Reply, StreamingStarted, UsableTradePairsSet}
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.{Exchange, ExchangeAccountDataChannelInit, ExchangePublicDataChannelInit, ExchangePublicDataInquirerInit}
import org.purevalue.arbitrage.{Config, UserRootGuardian}


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
  case class UsableTradePairsSet(exchange: String) extends Reply
  case class StreamingStarted(exchange: String) extends Reply
}
class TradeRoomInitCoordinator(context: ActorContext[TradeRoomInitCoordinator.Reply],
                               config: Config,
                               parent: ActorRef[UserRootGuardian.Reply])
  extends AbstractBehavior[TradeRoomInitCoordinator.Reply](context) {

  // @formatter:off
  var allTradePairs:      Map[String, Set[TradePair]] = Map()
  var usableTradePairs:   Map[String, Set[TradePair]] = Map()
  var exchanges:          Map[String, ActorRef[Exchange.Message]] = Map()
  // @formatter:on

  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      BinancePublicDataInquirer,
      BinancePublicDataChannel,
      BinanceAccountDataChannel
    ),
    "bitfinex" -> ExchangeInitStuff(
      BitfinexPublicDataInquirer,
      BitfinexPublicDataChannel,
      BitfinexAccountDataChannel
    ),
    "coinbase" -> ExchangeInitStuff(
      CoinbasePublicDataInquirer,
      CoinbasePublicDataChannel,
      CoinbaseAccountDataChannel
    )
  )

  // we want to keep only trade pairs
  // - [1] which are NOT connected to Fiat money
  // - [2] that are available on at least two exchanges
  // - [3] plus trade pairs, where one side is a part of [2] and the other side is a local reserve asset
  // - [4] plus all crypto-coin to USD equivalent coin pairs (for liquidity calculations & conversions)
  def determineUsableTradepairs(): Unit = {
    val allGlobalTradePairs = allTradePairs.values.flatten.toSet
    val globalArbitragePairs = allGlobalTradePairs.filter(e => allTradePairs.count(_._2.contains(e)) > 1)

    val arbitrageAssets = globalArbitragePairs.flatMap(_.involvedAssets)

    def condition3(exchange: String, pair: TradePair): Boolean = {
      (arbitrageAssets.contains(pair.baseAsset) && (pair.quoteAsset == config.exchanges(exchange).usdEquivalentCoin || config.exchanges(exchange).reserveAssets.contains(pair.quoteAsset))) ||
        (arbitrageAssets.contains(pair.quoteAsset) && pair.baseAsset == config.exchanges(exchange).usdEquivalentCoin || config.exchanges(exchange).reserveAssets.contains(pair.baseAsset))
    }

    def condition4(exchange: String, pair: TradePair): Boolean = {
      !pair.baseAsset.isFiat && pair.quoteAsset == config.exchanges(exchange).usdEquivalentCoin
    }

    usableTradePairs = allTradePairs
      .map(e => e._1 ->
        e._2.filter(x =>
          globalArbitragePairs.contains(x)
            || condition3(e._1, x)
            || condition4(e._1, x)
        ))

    for (exchange <- exchanges.keySet) {
      context.log.info(s"[$exchange] unusable trade pairs: ${(allTradePairs(exchange) -- usableTradePairs(exchange)).toSeq.sortBy(_.toString)}")
    }
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
    context.log.debug("TradeRoom initialized")
    val tradeRoom = context.spawn(TradeRoom(config, exchanges, usableTradePairs), "TradeRoom")
    parent ! UserRootGuardian.TradeRoomInitialized(tradeRoom)
    context.watch(tradeRoom)
    Behaviors.empty
  }

  def sendStartStreaming(): Behavior[Reply] = {
    var exchangesStreamingPending: Set[String] = exchanges.keySet

    exchanges.values.foreach { exchange =>
      exchange ! StartStreaming(context.self)
    }

    Behaviors.receiveMessage[Reply] {
      case StreamingStarted(exchange) =>
        exchangesStreamingPending -= exchange
        context.log.debug(s"streaming started on $exchange. Still waiting for $exchangesStreamingPending")
        if (exchangesStreamingPending.nonEmpty) Behaviors.same
        else initialized()
    }
  }

  def pushUsableTradePairs(): Behavior[Reply] = {
    exchanges.foreach {
      case (exchange, actor) => actor ! SetUsableTradePairs(usableTradePairs(exchange), context.self)
    }
    var exchangesReplied: Set[String] = Set()

    Behaviors.receiveMessage[Reply] {
      case UsableTradePairsSet(exchange) =>
        exchangesReplied += exchange
        if (exchangesReplied != exchanges.keySet) Behaviors.same
        else sendStartStreaming()
    }
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
        if (allTradePairs.keySet != exchanges.keySet) {
          Behaviors.same
        } else {
          determineUsableTradepairs()
          pushUsableTradePairs()
        }
    }
  }

  startExchanges() // parallel
  exchanges.keys.foreach(exchange =>
    exchanges(exchange) ! GetAllTradePairs(context.self)
  )
}

//  override def preStart(): Unit = {
//    startExchanges() // parallel
//    queryAllTradePairs()
//    determineUsableTradepairs()
//    pushUsableTradePairs()
//    sendStartStreaming()
//  }
//
//  override def receive: Receive = {
//    // @formatter:off
//
//    case Exchange.StreamingStarted(exchange) => onStreamingStarted(exchange)
//    case akka.actor.Status.Failure(cause)    => log.error(cause, "Failure received")
//    case msg                                 => log.error(s"unexpected message: $msg")
//    // @formatter:on
//  }


// TODO
//  override val supervisorStrategy: AllForOneStrategy = {
//    AllForOneStrategy() {
//      case _: Exception => Escalate
//    }
