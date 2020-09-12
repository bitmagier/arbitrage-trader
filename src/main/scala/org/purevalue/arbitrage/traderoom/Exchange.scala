package org.purevalue.arbitrage.traderoom

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.traderoom.Exchange.{GetTradePairs, RemoveTradePair, StartStreaming, TradePairs}
import org.purevalue.arbitrage.traderoom.ExchangeAccountDataManager.{CancelOrder, NewLimitOrder}
import org.purevalue.arbitrage.traderoom.ExchangeLiquidityManager.{LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.TradeRoom.{LiquidityTx, WalletUpdateTrigger}
import org.purevalue.arbitrage.util.Emoji
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

object Exchange {
  case class StartStreaming()
  case class Initialized(exchange: String)
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class RemoveTradePair(tradePair: TradePair)

  def props(exchangeName: String,
            config: ExchangeConfig,
            liquidityManagerConfig: LiquidityManagerConfig,
            tradeRoom: ActorRef,
            initStuff: ExchangeInitStuff,
            publicData: ExchangePublicData,
            accountData: ExchangeAccountData,
            referenceTicker: () => collection.Map[TradePair, Ticker],
            openLiquidityTx: () => Iterable[LiquidityTx]): Props =
    Props(new Exchange(exchangeName, config, liquidityManagerConfig, tradeRoom, initStuff, publicData, accountData, referenceTicker, openLiquidityTx))
}

case class Exchange(exchangeName: String,
                    config: ExchangeConfig,
                    liquidityManagerConfig: LiquidityManagerConfig,
                    tradeRoom: ActorRef,
                    initStuff: ExchangeInitStuff,
                    publicData: ExchangePublicData,
                    accountData: ExchangeAccountData,
                    referenceTicker: () => collection.Map[TradePair, Ticker],
                    openLiquidityTx: () => Iterable[LiquidityTx]) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val tradeSimulationMode: Boolean = Config.tradeRoom.tradeSimulation

  var publicDataInquirer: ActorRef = _
  var liquidityManager: ActorRef = _
  var tradeSimulator: Option[ActorRef] = None

  // dynamic
  var tradePairs: Set[TradePair] = _
  var publicDataManager: ActorRef = _
  var accountDataManager: ActorRef = _

  var publicDataManagerInitialized = false
  var accountDataManagerInitialized = false
  var liquidityManagerInitialized = false

  def initialized: Boolean = publicDataManagerInitialized && accountDataManagerInitialized && liquidityManagerInitialized


  def initPublicDataManager(): Unit = {
    publicDataManager = context.actorOf(
      ExchangePublicDataManager.props(config, tradePairs, publicDataInquirer, self, initStuff.exchangePublicDataChannelProps, publicData),
      s"$exchangeName-PublicDataManager")
  }

  def initAccountDataManager(): Unit = {
    accountDataManager = context.actorOf(
      ExchangeAccountDataManager.props(
        config,
        self,
        publicDataInquirer,
        tradeRoom,
        initStuff.exchangeAccountDataChannelProps,
        accountData),
      s"${config.exchangeName}.AccountDataManager")

    if (tradeSimulationMode)
      tradeSimulator = Some(context.actorOf(TradeSimulator.props(config, accountDataManager), s"${config.exchangeName}-TradeSimulator"))
  }

  def removeTradePairBeforeInitialized(tp: TradePair): Unit = {
    if (initialized) throw new RuntimeException("bad timing")
    tradePairs = tradePairs - tp
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  override def preStart(): Unit = {
    log.info(s"Initializing Exchange $exchangeName " +
      s"${if (tradeSimulationMode) " in TRADE-SIMULATION mode"}" +
      s"${if (config.doNotTouchTheseAssets.nonEmpty) s" DO-NOT-TOUCH: ${config.doNotTouchTheseAssets.mkString(",")}"}")
    publicDataInquirer = context.actorOf(initStuff.publicDataInquirerProps(config), s"$exchangeName-PublicDataInquirer")

    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    tradePairs = Await.result(
      (publicDataInquirer ? GetTradePairs()).mapTo[TradePairs].map(_.value),
      timeout.duration.plus(500.millis))
      .filter(e => config.tradeAssets.contains(e.baseAsset) && config.tradeAssets.contains(e.quoteAsset))

    log.info(s"$exchangeName: ${tradePairs.size} TradePairs: ${tradePairs.toSeq.sortBy(e => e.toString)}")
  }

  def checkValidity(o: NewLimitOrder): Unit = {
    if (config.doNotTouchTheseAssets.contains(o.o.tradePair.baseAsset) || config.doNotTouchTheseAssets.contains(o.o.tradePair.quoteAsset))
      throw new IllegalArgumentException("Order with DO-NOT-TOUCH asset")
  }

  def initLiquidityManager(): Unit = {
    liquidityManager = context.actorOf(
      ExchangeLiquidityManager.props(
        liquidityManagerConfig, config, tradeRoom, publicData.readonly, accountData.wallet, referenceTicker, openLiquidityTx))
  }

  def eventuallyInitialized(): Unit = {
    if (publicDataManagerInitialized && accountDataManagerInitialized) {
      initLiquidityManager()
      liquidityManagerInitialized = true
      log.info(s"${Emoji.Robot}  [$exchangeName]: All data streams initialized and running")
      tradeRoom ! Exchange.Initialized(exchangeName)
    }
  }

  override def receive: Receive = {

    // Messages from TradeRoom side
    case GetTradePairs() => sender() ! tradePairs

    case c: CancelOrder =>
      if (tradeSimulationMode) tradeSimulator.get.forward(c)
      else accountDataManager.forward(c)

    case o: NewLimitOrder =>
      checkValidity(o)
      if (tradeSimulationMode) tradeSimulator.get.forward(o)
      else accountDataManager.forward(o)

    case l: LiquidityRequest => liquidityManager.forward(l)
    case c: LiquidityLockClearance => liquidityManager.forward(c)

    case t: WalletUpdateTrigger => if (initialized) liquidityManager.forward(t)

    // removes tradepair before streaming has started
    case RemoveTradePair(tp) =>
      removeTradePairBeforeInitialized(tp)
      sender() ! true

    case StartStreaming() =>
      initPublicDataManager()
      if (config.secrets.apiKey.isEmpty || config.secrets.apiSecretKey.isEmpty)
        log.warn(s"Will NOT start AccountDataManager for exchange ${config.exchangeName} because API-Key is not set")
      else initAccountDataManager()

    // Messages from TradePairDataManager

    case ExchangePublicDataManager.Initialized() =>
      if (log.isTraceEnabled) log.trace(s"[$exchangeName]: PublicDataManager initialized")
      publicDataManagerInitialized = true
      eventuallyInitialized()

    case ExchangeAccountDataManager.Initialized() =>
      if (log.isTraceEnabled) log.trace(s"[$exchangeName]: AccountDataManager initialized")
      accountDataManagerInitialized = true
      eventuallyInitialized()

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
