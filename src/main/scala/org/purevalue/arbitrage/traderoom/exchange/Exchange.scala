package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}

import akka.Done
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, OneForOneStrategy, PoisonPill, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, NewLimitOrder}
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.traderoom.Asset.USDT
import org.purevalue.arbitrage.traderoom.TradeRoom.{JoinTradeRoom, OrderRef, WalletUpdateTrigger}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange.{GetTradePairs, HouseKeeping, RemoveTradePair, TradePairs}
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrderFailed, PioneerOrderSucceeded}
import org.purevalue.arbitrage.util.{Emoji, InitSequence, InitStep}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Promise}

object Exchange {
  case class TradeRoomJoined(exchange: String)
  case class HouseKeeping()
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class RemoveTradePair(tradePair: TradePair)

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

  def initPublicDataManager(): Unit = {
    publicDataManager = context.actorOf(
      ExchangePublicDataManager.props(globalConfig, exchangeConfig, tradeRoomConfig, tradePairs, publicDataInquirer, self, initStuff.exchangePublicDataChannelProps, publicData),
      s"PublicDataManager-$exchangeName")
  }

  def removeTradePairBeforeInitialized(tp: TradePair): Unit = {
    tradePairs = tradePairs - tp
  }

  def initLiquidityManager(): Unit = {
    liquidityManager = context.actorOf(
      LiquidityManager.props(
        tradeRoomConfig.liquidityManager, exchangeConfig, publicData.readonly, accountData.wallet))
  }

  def checkIfBalanceIsSufficientForTrading(): Unit = {
    def isBalanceSufficient: Boolean = {
      accountData.wallet.balance.contains(USDT) &&
        accountData.wallet.balance(USDT).amountAvailable >= tradeRoomConfig.pioneerOrderValueUSDT &&
        accountData.wallet.liquidCryptoValueSum(USDT, publicData.ticker).amount >= tradeRoomConfig.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSDT
    }

    // wait maximal 3 seconds until all wallet entries arrive
    val timeout: Instant = Instant.now.plusSeconds(3)
    val minRequiredBalance: Double = tradeRoomConfig.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSDT
    do {
      if (isBalanceSufficient) return
      Thread.sleep(250)
    } while (Instant.now.isBefore(timeout))

    throw new RuntimeException(s"Insufficient balance for trading on $exchangeName. " +
      s"Expectation is to have at least ${tradeRoomConfig.pioneerOrderValueUSDT} USDT for the pioneer transaction " +
      s"and a cumulated amount of at least $minRequiredBalance USDT available for trading. " +
      s"Wallet:\n${accountData.wallet.balance.mkString("\n")}")
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
        s"AccountDataManager-$exchangeName")
      if (tradeSimulationMode) {
        tradeSimulator = Some(context.actorOf(TradeSimulator.props(exchangeConfig, accountDataManager), s"TradeSimulator-$exchangeName"))
      }
    }
  }

  def initiatePioneerOrder(): Unit = {
    pioneerOrderRunner = context.actorOf(PioneerOrderRunner.props(
      globalConfig,
      tradeRoomConfig,
      exchangeName,
      self,
      publicData.ticker,
      (orderRef: OrderRef) => accountData.activeOrders.get(orderRef)
    ), s"PioneerOrderRunner-$exchangeName")
  }

  def joinTradeRoom(j: JoinTradeRoom): Unit = {
    this.tradeRoom = Some(j.tradeRoom)
    accountDataManager.forward(j) // async, not waiting for the replied Done (which is only needed in test)
    liquidityManager.forward(j) // async, not waiting for the replied Done (which is only needed in test)
    joinedTradeRoom.success _
  }

  def switchToInitializedMode(): Unit = {
    houseKeepingSchedule = actorSystem.scheduler.scheduleAtFixedRate(0.seconds, 1.minute, self, HouseKeeping())
    context.become(initializedModeReceive)
    log.info(s"${Emoji.Excited}  [$exchangeName]: completely initialized and running")
    tradeRoom.get ! Exchange.TradeRoomJoined(exchangeName)
  }


  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  val startStreamingRequested: Promise[Nothing] = Promise()
  val publicDataManagerInitialized: Promise[Nothing] = Promise()
  val accountDataManagerInitialized: Promise[Nothing] = Promise()
  val walletInitialized: Promise[Nothing] = Promise()
  val joinedTradeRoom: Promise[Nothing] = Promise()
  val pioneerOrderSucceeded: Promise[Nothing] = Promise()

  override def preStart(): Unit = {
    try {
      log.info(s"Initializing Exchange $exchangeName " +
        s"${if (tradeSimulationMode) " in TRADE-SIMULATION mode"}" +
        s"${if (exchangeConfig.doNotTouchTheseAssets.nonEmpty) s" DO-NOT-TOUCH: ${exchangeConfig.doNotTouchTheseAssets.mkString(",")}"}")

      val maxWaitTime = globalConfig.internalCommunicationTimeoutDuringInit.duration
      val initSequence = new InitSequence(List(
        InitStep("start public-data-inquirer", () => startPublicDataInquirer()),
        InitStep("wait for start-streaming message", () => Await.ready(startStreamingRequested.future, maxWaitTime)),
        InitStep("start account-data-manager", () => startAccountDataManager()),
        InitStep("start public-data-manager", () => initPublicDataManager()),
        InitStep("wait until account-data-manager initialized", () => Await.ready(accountDataManagerInitialized.future, maxWaitTime)),
        InitStep("wait until wallet data arrives", () => Await.ready(walletInitialized.future, maxWaitTime)),
        InitStep("check if balance is sufficient for trading", () => checkIfBalanceIsSufficientForTrading()),
        InitStep("wait until public-data-manager initialized", () => Await.ready(publicDataManagerInitialized.future, maxWaitTime)),
        InitStep(s"initiate pioneer order (${tradeRoomConfig.pioneerOrderValueUSDT} USDT -> Bitcoin)", () => initiatePioneerOrder()),
        InitStep("waiting for pioneer order to succeed", () => Await.ready(pioneerOrderSucceeded.future, maxWaitTime)),
        InitStep("wait until joined trade-room", () => Await.ready(joinedTradeRoom.future, maxWaitTime)),
        InitStep("init liquidity manager", () => initLiquidityManager())
      ),
        () => switchToInitializedMode(),
        exception => {
          log.error(exception.message, exception)
          self ! PoisonPill // TODO coordinated shutdown
        }
      )
      executionContext.execute(() => initSequence.run())
    } catch {
      case e: Exception => log.error(s"$exchangeName: preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case GetTradePairs()                          => sender() ! tradePairs
    case RemoveTradePair(tp)                      => removeTradePairBeforeInitialized(tp); sender() ! true
    case ExchangePublicDataManager.Initialized()  => publicDataManagerInitialized.success _
    case ExchangeAccountDataManager.Initialized() => accountDataManagerInitialized.success _
    case PioneerOrderSucceeded()                  => pioneerOrderSucceeded.success _
    case PioneerOrderFailed(e)                    => pioneerOrderSucceeded.failure(e)
    case j: JoinTradeRoom                         => joinTradeRoom(j)
    case WalletUpdateTrigger(_)                   => walletInitialized.success _
    case o: NewLimitOrder                         => onNewLimitOrder(o) // needed only for pioneer order runner
    case Done                                     => // ignoring Done from cascaded JoinTradeRoom
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
    val lastSeen: Instant = (publicData.age.heartbeatTS.toSeq ++ Seq(publicData.age.tickerTS)).max
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

  // @formatter:off
  def initializedModeReceive: Receive = {
    case c: CancelOrder            => onCancelOrder(c)
    case o: NewLimitOrder          => onNewLimitOrder(o)
    case l: LiquidityRequest       => liquidityManager.forward(l)
    case c: LiquidityLockClearance => liquidityManager.forward(c)
    case t: WalletUpdateTrigger    => // (unused) liquidityManager.forward(t)
    case HouseKeeping()            => houseKeeping()
    case s: TradeRoom.Stop         => onStop(s)
    case Status.Failure(cause)     => log.error("received failure", cause)
  }
  // @formatter:off
}
