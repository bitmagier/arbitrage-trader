package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, PoisonPill, Props, Status}
import akka.pattern.gracefulStop
import org.purevalue.arbitrage.Exchange.{GetTradePairs, RemoveTradePair, TradePairs}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}


object Exchange {
  case class Initialized(exchange:String)
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class RemoveTradePair(tradePair: TradePair)

  def props(exchangeName: String,
            config: ExchangeConfig,
            tradeRoom: ActorRef,
            exchangeDataChannel: ActorRef,
            tpDataChannelPropsInit: Function1[TPDataChannelPropsParams, Props],
            tpData: ExchangeTPData,
            accountData: ExchangeAccountData): Props =
    Props(new Exchange(exchangeName, config, tradeRoom, exchangeDataChannel, tpDataChannelPropsInit, tpData, accountData))
}

case class Exchange(exchangeName: String,
                    config: ExchangeConfig,
                    tradeRoom: ActorRef,
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

  def removeTradePair(tp: TradePair): Unit = {
    try {
      val stopped: Future[Boolean] = gracefulStop(tpDataManagers(tp), AppConfig.tradeRoom.gracefulStopTimeout.duration, TPDataManager.Stop())
      Await.result(stopped, AppConfig.tradeRoom.gracefulStopTimeout.duration)
      log.debug(s"$exchangeName-TPDataManager for $tp stopped (${stopped.value.get})")
    } catch {
      case t: Throwable =>
        log.warn(s"Catched $t: So we are forced, to send a PosionPill to ${tpDataManagers(tp)} because gracefulStop did not work")
        tpDataManagers(tp) ! PoisonPill
    } finally {
      tpDataManagers = tpDataManagers - tp
    }
    tpData.ticker.remove(tp)
    tpData.extendedTicker.remove(tp)
    tpData.orderBook.remove(tp)

    tradePairs = tradePairs - tp
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
    case GetTradePairs() =>
      if (!initialized) throw new RuntimeException("bad timing")
      sender() ! tradePairs

    case RemoveTradePair(tp) =>
      if (!initialized) throw new RuntimeException("bad timing")
      removeTradePair(tp)
      sender() ! true

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
        log.info(s"${Emoji.Robot}  [$exchangeName]: All TradePair data initialized and running")
        tradeRoom ! Exchange.Initialized(exchangeName)
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// find out why Ticker of removed TradePairs are stilled feeded with data after removal & fix that
// TODO restart ExchangeTPDataManager when ticker data gets much too old (like 30 sec)