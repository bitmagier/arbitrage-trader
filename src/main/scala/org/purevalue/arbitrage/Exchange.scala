package org.purevalue.arbitrage

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage.Exchange.{GetTradePairs, RemoveTradePair, StartStreaming, TradePairs}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}


object Exchange {
  case class StartStreaming()
  case class Initialized(exchange:String)
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class RemoveTradePair(tradePair: TradePair)
  case class RemoveRunningTradePair(tradePair: TradePair)

  def props(exchangeName: String,
            config: ExchangeConfig,
            tradeRoom: ActorRef,
            exchangePublicDataInquirer: ActorRef,
            exchangePublicTPDataChannelPropsInit: Function1[ExchangePublicTPDataChannelPropsParams, Props],
            exchangeAccountDataChannelPropsInit: () => Props,
            tpData: ExchangeTPData,
            accountData: ExchangeAccountData): Props =
    Props(new Exchange(exchangeName, config, tradeRoom, exchangePublicDataInquirer, exchangePublicTPDataChannelPropsInit,
      exchangeAccountDataChannelPropsInit, tpData, accountData))
}

case class Exchange(exchangeName: String,
                    config: ExchangeConfig,
                    tradeRoom: ActorRef,
                    exchangePublicDataInquirer: ActorRef,
                    exchangePublicTPDataChannelPropsInit: Function1[ExchangePublicTPDataChannelPropsParams, Props],
                    exchangeAccountDataChannelPropsInit: () => Props,
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
          ExchangeTPDataManager.props(config, tp, exchangePublicDataInquirer, self, exchangePublicTPDataChannelPropsInit, tpData),
          s"$exchangeName.TPDataManager-${tp.baseAsset.officialSymbol}-${tp.quoteAsset.officialSymbol}"))
    }
  }

  def initAccountDataManager(): Unit = {
    accountDataManager = context.actorOf(ExchangeAccountDataManager.props(config, exchangeAccountDataChannelPropsInit, accountData),
      s"${config.exchangeName}.AccountDataManager")
  }

  def removeTradePairBeforeInitialized(tp:TradePair): Unit = {
    if (initialized) throw new RuntimeException("bad timing")
    tradePairs = tradePairs - tp
  }

//  def removeRunningTradePair(tp: TradePair): Unit = {
//    if (!initialized) throw new RuntimeException("bad timing")
//    try {
//      val stopped: Future[Boolean] = gracefulStop(tpDataManagers(tp), Config.gracefulStopTimeout.duration, TPDataManager.Stop())
//      Await.result(stopped, Config.gracefulStopTimeout.duration)
//      log.debug(s"$exchangeName-TPDataManager for $tp stopped (${stopped.value.get})")
//    } catch {
//      case t: Throwable =>
//        log.warn(s"Catched $t: So we are forced, to send a PosionPill to ${tpDataManagers(tp)} because gracefulStop did not work")
//        tpDataManagers(tp) ! PoisonPill
//    } finally {
//      tpDataManagers = tpDataManagers - tp
//    }
//    tpData.ticker.remove(tp)
//    tpData.extendedTicker.remove(tp)
//    tpData.orderBook.remove(tp)
//
//    tradePairs = tradePairs - tp
//  }
//

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  override def preStart(): Unit = {
    log.info(s"Initializing Exchange $exchangeName")
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    tradePairs = Await.result((exchangePublicDataInquirer ? GetTradePairs()).mapTo[TradePairs], timeout.duration.plus(500.millis)).value
    log.info(s"$exchangeName: ${tradePairs.size} TradePairs: ${tradePairs.toSeq.sortBy(e => e.toString)}")
  }

  override def receive: Receive = {

    // Messages from TradeRoom
    case GetTradePairs() =>
      sender() ! tradePairs

    // removes tradepair before streaming has started
    case RemoveTradePair(tp) =>
        removeTradePairBeforeInitialized(tp)
        sender() ! true

    case StartStreaming() =>
      initTradePairBasedDataManagers()
      if (exchangeAccountDataChannelPropsInit != null) initAccountDataManager() // TODO remove nullable condition

    // tested, but currently unused - maybe we need that later
//    case RemoveRunningTradePair(tp) =>
//      removeRunningTradePair(tp)
//      sender() ! true

    // Messages from TradePairDataManager

    case ExchangeTPDataManager.Initialized(t) =>
      tpDataInitPending -= t
      log.debug(s"[$exchangeName]: [$t] initialized. Still pending: $tpDataInitPending")
      if (tpDataInitPending.isEmpty) {
        log.info(s"${Emoji.Robot}  [$exchangeName]: All TradePair data streams initialized and running")
        tradeRoom ! Exchange.Initialized(exchangeName)
      }

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}

// find out why Ticker of removed TradePairs are stilled feeded with data after removal & fix that
// TODO restart ExchangeTPDataManager when ticker data gets much too old (like 30 sec)