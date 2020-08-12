package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Status}
import org.purevalue.arbitrage.Exchange.{GetTradePairs, IsInitialized, TradePairs}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt


object Exchange {
  case class IsInitialized()
  case class IsInitializedResponse(initialized: Boolean)
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])

  def props(exchangeName: String,
            config: ExchangeConfig,
            exchangeDataChannel: ActorRef,
            tpDataChannelPropsInit: Function1[TPDataChannelPropsParams, Props],
            tpData: ExchangeTPData,
            accountData: ExchangeAccountData): Props =
    Props(new Exchange(exchangeName, config, exchangeDataChannel, tpDataChannelPropsInit, tpData, accountData))
}

case class Exchange(exchangeName: String,
                    config: ExchangeConfig,
                    exchangeDataChannel: ActorRef,
                    tpDataChannelPropsInit: Function1[TPDataChannelPropsParams, Props],
                    tpData: ExchangeTPData,
                    accountData: ExchangeAccountData) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  // dynamic
  var tradePairs: Set[TradePair] = _
  var tpDataManagers: Map[TradePair, ActorRef] = Map()
  var tpDataInitPending: Set[TradePair] = _
  var accountDataManager: ActorRef = _

  def initialized: Boolean = tpDataInitPending != null && tpDataInitPending.isEmpty

  def initTradePairBasedDataManagers(): Unit = {
    tpDataInitPending = tradePairs
    for (tp <- tradePairs) {
      tpDataManagers = tpDataManagers +
        (tp -> context.actorOf(
          TPDataManager.props(config, tp, exchangeDataChannel, self, tpDataChannelPropsInit, tpData),
          s"$exchangeName.TPDataManager-${tp.baseAsset.officialSymbol}-${tp.quoteAsset.officialSymbol}"))
    }
  }

  def initAccountDataManager(): Unit = {
    accountDataManager = context.actorOf(ExchangeAccountDataManager.props(config, accountData),
      s"${config.exchangeName}.AccountDataManager")
  }


  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }


  override def preStart(): Unit = {
    log.info(s"Initializing Exchange $exchangeName")
    exchangeDataChannel ! GetTradePairs()
  }

  override def receive: Receive = {

    // Messages from TradeRoom
    case IsInitialized() =>
      val result: Boolean = initialized
      if (!result) {
        log.debug(s"[$exchangeName] initialization pending: $tpDataInitPending")
      }
      sender() ! Exchange.IsInitializedResponse(result)

    // Messages from ExchangeDataChannel

    case TradePairs(t) =>
      tradePairs = t
      log.info(s"$exchangeName: ${tradePairs.size} TradePairs: $tradePairs")
      initTradePairBasedDataManagers()
      initAccountDataManager()

    // Messages from TradePairDataManager

    case TPDataManager.Initialized(t) =>
      tpDataInitPending -= t
      log.debug(s"[$exchangeName]: [$t] initialized. Still pending: $tpDataInitPending")
      if (tpDataInitPending.isEmpty) {
        log.info(s"${Emoji.Robot} [$exchangeName]: All TradePair data initialized and running")
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
