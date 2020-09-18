package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.Done
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Kill, OneForOneStrategy, PoisonPill, Props, Status}
import akka.pattern.ask
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager.{CancelOrder, NewLimitOrder}
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.traderoom.Asset.USDT
import org.purevalue.arbitrage.traderoom.TradeRoom.{JoinTradeRoom, OrderRef, TradeRoomJoined}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLockClearance, LiquidityRequest}
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrderFailed, PioneerOrderSucceeded}
import org.purevalue.arbitrage.util.{Emoji, InitSequence, InitStep}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object Exchange {
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class RemoveTradePair(tradePair: TradePair)
  case class StartStreaming()
  case class StreamingStarted(exchange: String)
  case class WalletUpdateTrigger()
  case class OrderUpdateTrigger(ref: OrderRef) // status of an order has changed
  case class HouseKeeping()

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
    def balanceSufficient: Boolean =
      accountData.wallet.balance.contains(USDT) &&
        accountData.wallet.balance(USDT).amountAvailable >= tradeRoomConfig.pioneerOrderValueUSDT &&
        accountData.wallet.liquidCryptoValueSum(USDT, publicData.ticker).amount >= tradeRoomConfig.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSDT

    val minRequiredBalance: Double = tradeRoomConfig.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSDT

    if (!balanceSufficient) {
      throw new RuntimeException(s"Insufficient balance for trading on $exchangeName. " +
        s"Expectation is to have at least ${tradeRoomConfig.pioneerOrderValueUSDT} USDT for the pioneer order " +
        s"and a cumulated amount of at least $minRequiredBalance USDT available for trading. " +
        s"Wallet:\n${accountData.wallet.balance.values.mkString("\n")}")
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
    liquidityManager.forward(j) // async, not waiting for the replied Done (which is only needed in test)
    joinedTradeRoom.countDown()
  }

  def switchToInitializedMode(): Unit = {
    houseKeepingSchedule = actorSystem.scheduler.scheduleAtFixedRate(20.seconds, 1.minute, self, HouseKeeping())
    context.become(initializedModeReceive)
    log.info(s"${Emoji.Excited}  [$exchangeName]: completely initialized and running")
    tradeRoom.get ! TradeRoomJoined(exchangeName)
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 10.minutes, loggingEnabled = true) {
      case _ => Restart
    }

  val accountDataManagerInitialized: CountDownLatch = new CountDownLatch(1)
  val publicDataManagerInitialized: CountDownLatch = new CountDownLatch(1)
  val walletInitialized: CountDownLatch = new CountDownLatch(1)
  val joinedTradeRoom: CountDownLatch = new CountDownLatch(1)
  val pioneerOrderSucceeded: CountDownLatch = new CountDownLatch(1)


  def startStreaming(): Unit = {
    val sendStreamingStartedResponseTo = sender()
    val maxWaitTime = globalConfig.internalCommunicationTimeoutDuringInit.duration
    val initSequence = new InitSequence(
      log,
      List(
        InitStep("start account-data-manager", () => startAccountDataManager()),
        InitStep("start public-data-manager", () => initPublicDataManager()),
        InitStep("wait until account-data-manager initialized", () => accountDataManagerInitialized.await(maxWaitTime.toMillis, TimeUnit.MILLISECONDS)),
        InitStep("wait until wallet data arrives", () => walletInitialized.await(maxWaitTime.toMillis, TimeUnit.MILLISECONDS)),
        InitStep("wait until public-data-manager initialized", () => publicDataManagerInitialized.await(maxWaitTime.toMillis, TimeUnit.MILLISECONDS)),
        InitStep("check if balance is sufficient for trading", () => checkIfBalanceIsSufficientForTrading()),
        InitStep(s"initiate pioneer order (${tradeRoomConfig.pioneerOrderValueUSDT} USDT -> Bitcoin)", () => initiatePioneerOrder()),
        InitStep("waiting for pioneer order to succeed", () => pioneerOrderSucceeded.await(maxWaitTime.toMillis, TimeUnit.MILLISECONDS)),
        InitStep("send streaming-started", () => sendStreamingStartedResponseTo ! StreamingStarted(exchangeName)),
        InitStep("wait until joined trade-room", () => joinedTradeRoom.await(maxWaitTime.toMillis, TimeUnit.MILLISECONDS)),
        InitStep("init liquidity manager", () => initLiquidityManager())
      ))

    Future(initSequence.run()).onComplete {
      case Success(_) => switchToInitializedMode()
      case Failure(e) =>
        log.error("Init sequence failed", e)
        self ! PoisonPill // TODO coordinated shutdown
    }
  }

  override def preStart(): Unit = {
    try {
      log.info(s"Initializing Exchange $exchangeName" +
        s"${if (tradeSimulationMode) " in TRADE-SIMULATION mode"}" +
        s"${if (exchangeConfig.doNotTouchTheseAssets.nonEmpty) s" DoNotTouch: ${exchangeConfig.doNotTouchTheseAssets.mkString(",")}"}")

      startPublicDataInquirer()

    } catch {
      case e: Exception => log.error(s"$exchangeName: preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case StartStreaming()                         => startStreaming()
    case GetTradePairs()                          => sender() ! tradePairs
    case RemoveTradePair(tp)                      => removeTradePairBeforeInitialized(tp); sender() ! true
    case ExchangePublicDataManager.Initialized()  => publicDataManagerInitialized.countDown()
    case ExchangeAccountDataManager.Initialized() => accountDataManagerInitialized.countDown()
    case PioneerOrderSucceeded()                  => pioneerOrderSucceeded.countDown()
    case PioneerOrderFailed(e)                    => pioneerOrderSucceeded.countDown()
    case j: JoinTradeRoom                         => joinTradeRoom(j)
    case WalletUpdateTrigger()                    => if (walletInitialized.getCount > 0) walletInitialized.countDown()
    case t: OrderUpdateTrigger                    => if (tradeRoom.isDefined) tradeRoom.get.forward(t)
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
    case _: WalletUpdateTrigger    => // currently unused
    case t: OrderUpdateTrigger     => tradeRoom.get.forward(t)

    case HouseKeeping()            => houseKeeping()
    case s: TradeRoom.Stop         => onStop(s)
    case Status.Failure(cause)     => log.error("received failure", cause)
  }
  // @formatter:off
}
