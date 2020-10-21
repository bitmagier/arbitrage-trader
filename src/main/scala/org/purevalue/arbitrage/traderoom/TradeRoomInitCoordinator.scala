package org.purevalue.arbitrage.traderoom

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Config
import org.purevalue.arbitrage.adapter.binance.{BinanceAccountDataChannel, BinancePublicDataChannel, BinancePublicDataInquirer}
import org.purevalue.arbitrage.adapter.bitfinex.{BitfinexAccountDataChannel, BitfinexPublicDataChannel, BitfinexPublicDataInquirer}
import org.purevalue.arbitrage.adapter.coinbase.{CoinbaseAccountDataChannel, CoinbasePublicDataChannel, CoinbasePublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator.InitializedTradeRoom
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.{Exchange, ExchangeAccountDataChannelInit, ExchangePublicDataChannelInit, ExchangePublicDataInquirerInit}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt


case class ExchangeInitStuff(exchangePublicDataInquirerProps: ExchangePublicDataInquirerInit,
                             exchangePublicDataChannelProps: ExchangePublicDataChannelInit,
                             exchangeAccountDataChannelProps: ExchangeAccountDataChannelInit)

object TradeRoomInitCoordinator {
  case class InitializedTradeRoom(tradeRoom: ActorRef)
  def props(config: Config,
            parent: ActorRef): Props = Props(new TradeRoomInitCoordinator(config, parent))
}
class TradeRoomInitCoordinator(val config: Config,
                               val parent: ActorRef) extends Actor with ActorLogging {

  // @formatter:off
  var allTradePairs:      Map[String, Set[TradePair]] = Map()
  var usableTradePairs:   Map[String, Set[TradePair]] = Map()
  var exchanges:          Map[String, ActorRef] = Map()
  // @formatter:on

  val AllExchanges: Map[String, ExchangeInitStuff] = Map(
    "binance" -> ExchangeInitStuff(
      BinancePublicDataInquirer.props,
      BinancePublicDataChannel.props,
      BinanceAccountDataChannel.props
    ),
    "bitfinex" -> ExchangeInitStuff(
      BitfinexPublicDataInquirer.props,
      BitfinexPublicDataChannel.props,
      BitfinexAccountDataChannel.props
    ),
    "coinbase" -> ExchangeInitStuff(
      CoinbasePublicDataInquirer.props,
      CoinbasePublicDataChannel.props,
      CoinbaseAccountDataChannel.props
    )
  )

  def queryAllTradePairs(exchange: String): Set[TradePair] = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    Await.result((exchanges(exchange) ? GetAllTradePairs()).mapTo[Set[TradePair]], timeout.duration.plus(500.millis))
  }

  def queryAllTradePairs(): Unit = {
    for (exchange <- exchanges.keys) {
      allTradePairs = allTradePairs + (exchange -> queryAllTradePairs(exchange))
    }
  }

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
      log.info(s"[$exchange] unusable trade pairs: ${(allTradePairs(exchange) -- usableTradePairs(exchange)).toSeq.sortBy(_.toString)}")
    }
  }

  def pushUsableTradePairs(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    exchanges.foreach {
      case (exchange, actor) => Await.ready(actor ? SetUsableTradePairs(usableTradePairs(exchange)), timeout.duration.plus(500.millis))
    }
  }

  def startExchange(exchangeName: String, exchangeInit: ExchangeInitStuff): Unit = {
    exchanges = exchanges +
      (exchangeName -> context.actorOf(
        Exchange.props(
          exchangeName,
          config,
          config.exchanges(exchangeName),
          exchangeInit
        ), "Exchange-" + exchangeName))
  }

  def startExchanges(): Unit = {
    for (name: String <- config.exchanges.keys) {
      startExchange(name, AllExchanges(name))
    }
  }

  var exchangesStreamingPending: Set[String] = _

  def sendStartStreaming(): Unit = {
    exchangesStreamingPending = exchanges.keySet
    exchanges.values.foreach { exchange =>
      exchange ! StartStreaming()
    }
  }

  def onInitialized(): Unit = {
    log.debug("TradeRoom initialized")
    val tradeRoom = context.actorOf(TradeRoom.props(config, exchanges, usableTradePairs), "TradeRoom")
    parent ! InitializedTradeRoom(tradeRoom)
    context.watch(tradeRoom)
  }

  def onStreamingStarted(exchange: String): Unit = {
    exchangesStreamingPending = exchangesStreamingPending - exchange
    log.debug(s"streaming started on $exchange. Still waiting for $exchangesStreamingPending")
    if (exchangesStreamingPending.isEmpty) {
      onInitialized()
    }
  }

  override def preStart(): Unit = {
    startExchanges() // parallel
    queryAllTradePairs()
    determineUsableTradepairs()
    pushUsableTradePairs()
    sendStartStreaming()
  }

  override def receive: Receive = {
    // @formatter:off
    case Exchange.StreamingStarted(exchange) => onStreamingStarted(exchange)
    case akka.actor.Status.Failure(cause)    => log.error(cause, "Failure received")
    case msg                                 => log.error(s"unexpected message: $msg")
    // @formatter:on
  }

  override val supervisorStrategy: AllForOneStrategy = {
    AllForOneStrategy() {
      case _: Exception => Escalate
    }
  }
}
