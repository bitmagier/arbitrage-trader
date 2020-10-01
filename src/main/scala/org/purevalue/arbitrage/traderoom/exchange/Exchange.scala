package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, PoisonPill, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, NewLimitOrder}
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.traderoom.TradeRoom.{JoinTradeRoom, OrderRef, TradeRoomJoined}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrderFailed, PioneerOrderSucceeded}
import org.purevalue.arbitrage.util.{Emoji, InitSequence, InitStep, WaitingFor}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object Exchange {
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class RemoveTradePair(tradePair: TradePair)
  case class RemoveOrphanOrder(ref: OrderRef)
  case class StartStreaming()
  case class StreamingStarted(exchange: String)
  case class WalletUpdateTrigger()
  case class OrderUpdateTrigger(ref: OrderRef, resendCounter: Int = 0) // status of an order has changed
  case class HouseKeeping()
  case class SwitchToInitializedMode()

  type ExchangePublicDataInquirerInit = Function2[GlobalConfig, ExchangeConfig, Props]
  type ExchangePublicDataChannelInit = Function4[GlobalConfig, ExchangeConfig, ActorRef, ActorRef, Props]
  type ExchangeAccountDataChannelInit = Function4[GlobalConfig, ExchangeConfig, ActorRef, ActorRef, Props]

  def props(exchangeName: String,
            exchangeConfig: ExchangeConfig,
            globalConfig: GlobalConfig,
            tradeRoomConfig: TradeRoomConfig,
            initStuff: ExchangeInitStuff,
            publicData: ExchangePublicData,
            accountData: ExchangeAccountData): Props =
    Props(new Exchange(exchangeName, exchangeConfig, globalConfig, tradeRoomConfig, initStuff, publicData, accountData))
}

case class Exchange(exchangeName: String,
                    exchangeConfig: ExchangeConfig,
                    globalConfig: GlobalConfig,
                    tradeRoomConfig: TradeRoomConfig,
                    initStuff: ExchangeInitStuff,
                    publicData: ExchangePublicData,
                    accountData: ExchangeAccountData
                   ) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Exchange])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  var houseKeepingSchedule: Cancellable = _

  var tradeRoom: Option[ActorRef] = None

  var shutdownInitiated: Boolean = false
  val tradeSimulationMode: Boolean = tradeRoomConfig.tradeSimulation

  var publicDataInquirer: ActorRef = _
  var liquidityManager: ActorRef = _
  var tradeSimulator: Option[ActorRef] = None

  // dynamic
  var tradePairs: Set[TradePair] = _
  var publicDataManager: ActorRef = _
  var accountDataManager: ActorRef = _
  var pioneerOrderRunner: ActorRef = _

  def startPublicDataManager(): Unit = {
    publicDataManager = context.actorOf(
      ExchangePublicDataManager.props(globalConfig, exchangeConfig, tradeRoomConfig, tradePairs, publicDataInquirer, self, initStuff.exchangePublicDataChannelProps, publicData),
      s"ExchangePublicDataManager-$exchangeName")
  }

  def removeTradePairBeforeInitialized(tp: TradePair): Unit = {
    tradePairs = tradePairs - tp
  }

  def initLiquidityManager(j: JoinTradeRoom): Unit = {
    liquidityManager = context.actorOf(
      LiquidityManager.props(
        tradeRoomConfig.liquidityManager, exchangeConfig, tradePairs, publicData.readonly, accountData.wallet, j.tradeRoom, j.findOpenLiquidityTx, j.referenceTicker),
      s"LiquidityManager-$exchangeName"
    )
  }

  def checkIfBalanceIsSufficientForTrading(): Unit = {

    def balanceSufficient: Boolean =
      accountData.wallet.balance.contains(exchangeConfig.usdEquivalentCoin) &&
        accountData.wallet.balance(exchangeConfig.usdEquivalentCoin).amountAvailable >= tradeRoomConfig.pioneerOrderValueUSD &&
        accountData.wallet.liquidCryptoValueSum(exchangeConfig.usdEquivalentCoin, publicData.ticker).amount >= tradeRoomConfig.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD

    val minRequiredBalance: Double = tradeRoomConfig.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD

    if (!balanceSufficient) {
      throw new RuntimeException(s"Insufficient balance for trading on $exchangeName. " +
        s"Expectation is to have at least ${tradeRoomConfig.pioneerOrderValueUSD} ${exchangeConfig.usdEquivalentCoin.officialSymbol} for the pioneer order " +
        s"and a cumulated amount of at least $minRequiredBalance USD available for trading. " +
        s"Wallet:\n${accountData.wallet.balance.values.filter(_.amountAvailable > 0.0).mkString("\n")}")
    }
  }

  def startPublicDataInquirer(): Unit = {
    publicDataInquirer = context.actorOf(initStuff.exchangePublicDataInquirerProps(globalConfig, exchangeConfig), s"PublicDataInquirer-$exchangeName")

    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    tradePairs = Await.result(
      (publicDataInquirer ? GetTradePairs()).mapTo[TradePairs].map(_.value),
      timeout.duration.plus(500.millis))

    log.info(s"$exchangeName: ${tradePairs.size} TradePairs: ${tradePairs.toSeq.sortBy(e => e.toString)}")
  }

  def startAccountDataManager(): Unit = {
    if (shutdownInitiated) return
    if (exchangeConfig.secrets.apiKey.isEmpty || exchangeConfig.secrets.apiSecretKey.isEmpty) {
      throw new RuntimeException(s"Can not start AccountDataManager for exchange $exchangeName because API-Key is not set")
    } else {
      accountDataManager = context.actorOf(
        ExchangeAccountDataManager.props(
          globalConfig,
          exchangeConfig,
          tradeRoomConfig,
          self,
          publicDataInquirer,
          initStuff.exchangeAccountDataChannelProps,
          accountData),
        s"ExchangeAccountDataManager-$exchangeName")
      if (tradeSimulationMode) {
        tradeSimulator = Some(context.actorOf(TradeSimulator.props(exchangeConfig, accountDataManager), s"TradeSimulator-$exchangeName"))
      }
    }
  }

  def initiatePioneerOrders(): Unit = {
    pioneerOrderRunner = context.actorOf(PioneerOrderRunner.props(
      globalConfig,
      tradeRoomConfig,
      exchangeConfig,
      self,
      accountData,
      publicData), s"PioneerOrderRunner-$exchangeName")
  }

  def joinTradeRoom(j: JoinTradeRoom): Unit = {
    this.tradeRoom = Some(j.tradeRoom)
    initLiquidityManager(j)
    joinedTradeRoom.arrived()
  }

  def switchToInitializedMode(): Unit = {
    context.become(initializedModeReceive)
    houseKeepingSchedule = actorSystem.scheduler.scheduleAtFixedRate(20.seconds, 1.minute, self, HouseKeeping())
    log.info(s"${Emoji.Excited}  [$exchangeName] completely initialized and running")
    tradeRoom.get ! TradeRoomJoined(exchangeName)
  }

  val accountDataManagerInitialized: WaitingFor = WaitingFor()
  val publicDataManagerInitialized: WaitingFor = WaitingFor()
  val walletInitialized: WaitingFor = WaitingFor()
  val pioneerOrdersSucceeded: WaitingFor = WaitingFor()
  val joinedTradeRoom: WaitingFor = WaitingFor()

  def startStreaming(): Unit = {
    val sendStreamingStartedResponseTo = sender()
    val maxWaitTime = globalConfig.internalCommunicationTimeoutDuringInit.duration
    val pioneerOrderMaxWaitTime: FiniteDuration = maxWaitTime.plus(FiniteDuration(tradeRoomConfig.maxOrderLifetime.toMillis, TimeUnit.MILLISECONDS))
    val initSequence = new InitSequence(
      log,
      exchangeName,
      List(
        InitStep("start account-data-manager", () => startAccountDataManager()),
        InitStep("start public-data-manager", () => startPublicDataManager()),
        InitStep("wait until account-data-manager initialized", () => accountDataManagerInitialized.await(maxWaitTime)),
        InitStep("wait until wallet data arrives", () => { // wait another 2 seconds for all wallet entries to arrive (bitfinex)
          walletInitialized.await(maxWaitTime)
          Thread.sleep(2000)
        }),
        InitStep("wait until public-data-manager initialized", () => publicDataManagerInitialized.await(maxWaitTime)),
        InitStep("check if balance is sufficient for trading", () => checkIfBalanceIsSufficientForTrading()),
        InitStep("warmup channels for 3 seconds", () => Thread.sleep(3000)),
        InitStep(s"initiate pioneers", () => initiatePioneerOrders()),
        InitStep("waiting for pioneer orders to succeed", () => pioneerOrdersSucceeded.await(pioneerOrderMaxWaitTime)),
        InitStep("send streaming-started", () => sendStreamingStartedResponseTo ! StreamingStarted(exchangeName)),
        InitStep("wait until joined trade-room", () => joinedTradeRoom.await(maxWaitTime * 3))
      ))

    Future(initSequence.run()).onComplete {
      case Success(_) => self ! SwitchToInitializedMode()
      case Failure(e) =>
        log.error(s"[$exchangeName] Init sequence failed", e)
        self ! PoisonPill // TODO coordinated shutdown
    }
  }

  override def preStart(): Unit = {
    try {
      log.info(s"Initializing Exchange $exchangeName" +
        s"${if (tradeSimulationMode) " in TRADE-SIMULATION mode" else ""}" +
        s"${if (exchangeConfig.doNotTouchTheseAssets.nonEmpty) s" DoNotTouch: ${exchangeConfig.doNotTouchTheseAssets.mkString(", ")}" else ""}")

      startPublicDataInquirer()

    } catch {
      case e: Exception => log.error(s"$exchangeName: preStart failed", e)
      // TODO coordinated shudown
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case StartStreaming()                         => startStreaming()
    case GetTradePairs()                          => sender() ! tradePairs
    case RemoveTradePair(tp)                      => removeTradePairBeforeInitialized(tp); sender() ! true
    case ExchangePublicDataManager.Initialized()  => publicDataManagerInitialized.arrived()
    case ExchangeAccountDataManager.Initialized() => accountDataManagerInitialized.arrived()
    case PioneerOrderSucceeded()                  => pioneerOrdersSucceeded.arrived()
    case PioneerOrderFailed(e)                    => log.error(s"[$exchangeName] Pioneer order failed", e)
    case j: JoinTradeRoom                         => joinTradeRoom(j)
    case WalletUpdateTrigger()                    => if (!walletInitialized.isArrived) walletInitialized.arrived()
    case t: OrderUpdateTrigger                    => if (tradeRoom.isDefined) tradeRoom.get.forward(t)
    case o: NewLimitOrder                         => onNewLimitOrder(o) // needed only for pioneer order runner
    case c: CancelOrder                           => onCancelOrder(c) // needed only for pioneer order runner
    case Done                                     => // ignoring Done from cascaded JoinTradeRoom
    case SwitchToInitializedMode()                => switchToInitializedMode()
    case s: TradeRoom.Stop                        => onStop(s)
    case Status.Failure(cause)                    => log.error("received failure", cause)
  }
  // @formatter:on

  def checkValidity(o: OrderRequest): Unit = {
    if (exchangeConfig.doNotTouchTheseAssets.contains(o.tradePair.baseAsset)
      || exchangeConfig.doNotTouchTheseAssets.contains(o.tradePair.quoteAsset))
      throw new IllegalArgumentException("Order with DO-NOT-TOUCH asset")
  }

  def onNewLimitOrder(o: NewLimitOrder): Unit = {
    if (shutdownInitiated) return

    checkValidity(o.orderRequest)
    if (tradeSimulationMode) tradeSimulator.get.forward(o)
    else accountDataManager.forward(o)
  }

  def onCancelOrder(c: CancelOrder): Unit = {
    if (tradeSimulationMode) tradeSimulator.get.forward(c)
    else accountDataManager.forward(c)
  }

  /**
   * Will trigger a restart of the TradeRoom if stale data is found
   */
  def stalePublicDataWatch(): Unit = {
    val lastSeen: Instant = (publicData.age.heartbeatTS.toSeq ++ publicData.age.tickerTS.toSeq ++ publicData.age.orderBookTS.toSeq).max
    if (Duration.between(lastSeen, Instant.now).compareTo(tradeRoomConfig.restarExchangeWhenDataStreamIsOlderThan) > 0) {
      log.warn(s"${Emoji.Robot}  Killing Exchange actor ($exchangeName) because of outdated ticker data")
      self ! Kill
    }
  }

  def houseKeeping(): Unit = {
    stalePublicDataWatch()
  }

  def onStop(s: TradeRoom.Stop): Unit = {
    shutdownInitiated = true
    liquidityManager ! s
    self ! PoisonPill
  }

  def removeOrphanOrder(ref: OrderRef): Unit = {
    val order = accountData.activeOrders.remove(ref)
    accountData.activeOrders.remove(ref)
    log.info(s"[$exchangeName] cleaned up orphan finished order $order")
  }

  // @formatter:off
  def initializedModeReceive: Receive = {
    case o: NewLimitOrder          => onNewLimitOrder(o)
    case c: CancelOrder            => onCancelOrder(c)
    case l: LiquidityRequest       => liquidityManager.forward(l)
    case c: LiquidityLockClearance => liquidityManager.forward(c)
    case _: WalletUpdateTrigger    => // currently unused
    case t: OrderUpdateTrigger     => tradeRoom.get.forward(t)
    case RemoveOrphanOrder(ref)    => removeOrphanOrder(ref)
    case HouseKeeping()            => houseKeeping()
    case s: TradeRoom.Stop         => onStop(s)
    case Status.Failure(cause)     => log.error("received failure", cause); self ! PoisonPill
  }
  // @formatter:off
}
