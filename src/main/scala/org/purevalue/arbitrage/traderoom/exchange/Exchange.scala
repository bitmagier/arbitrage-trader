package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.{AccountDataChannel, PublicDataChannel, PublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradeRoom.{OrderRef, OrderUpdateTrigger, TradeRoomJoined}
import org.purevalue.arbitrage.traderoom.TradeRoomInitCoordinator.StreamingStarted
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.WorkingContext
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{LiquidityLockClearance, LiquidityLockRequest}
import org.purevalue.arbitrage.traderoom.{Asset, CryptoValue, ExchangeInitStuff, Order, OrderRequest, OrderUpdate, TradePair, TradeRoom, TradeRoomInitCoordinator, TradeSide, exchange}
import org.purevalue.arbitrage.util.{Emoji, InitSequence, InitStep, StaleDataException, WaitingFor}
import org.purevalue.arbitrage.{Config, ExchangeConfig, Main, UserRootGuardian}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

case class TickerSnapshot(exchange: String, ticker: Map[TradePair, Ticker])
case class DataTSSnapshot(exchange: String, heartbeatTS: Option[Instant], tickerTS: Option[Instant], orderBookTS: Option[Instant])

object Exchange {
  def apply(exchangeName: String,
            config: Config,
            exchangeConfig: ExchangeConfig,
            initStuff: ExchangeInitStuff):
  Behavior[Message] = {
    Behaviors.withTimers(timers =>
      Behaviors.setup(context => new Exchange(context, timers, exchangeName, config, exchangeConfig, initStuff)))
  }

  trait Message

  case class StartAccountDataManager() extends Message
  case class StartPublicDataChannel() extends Message
  case class InitiatePioneerOrders() extends Message

  case class IncomingPublicData(data: Seq[ExchangePublicStreamData]) extends Message
  case class IncomingAccountData(data: Seq[ExchangeAccountStreamData]) extends Message
  case class SimulatedAccountData(dataset: ExchangeAccountStreamData) extends Message

  case class GetAllTradePairs(replyTo: ActorRef[TradeRoomInitCoordinator.Reply]) extends Message
  case class GetTickerSnapshot(replyTo: ActorRef[TickerSnapshot]) extends Message
  case class GetWallet(replyTo: ActorRef[Wallet]) extends Message
  case class GetFullDataSnapshot(replyTo: ActorRef[TradeRoom.FullDataSnapshot]) extends Message
  case class GetActiveOrders(replyTo: ActorRef[Map[OrderRef, Order]]) extends Message
  case class GetWalletLiquidCrypto(replyTo: ActorRef[Seq[CryptoValue]]) extends Message

  case class SetUsableTradePairs(tradeable: Set[TradePair], replyTo: ActorRef[TradeRoomInitCoordinator.UsableTradePairsSet]) extends Message
  case class JoinTradeRoom(tradeRoom: ActorRef[TradeRoom.Message]) extends Message
  case class RemoveActiveOrder(ref: OrderRef) extends Message
  case class RemoveOrphanOrder(ref: OrderRef) extends Message

  case class StartStreaming(replyTo: ActorRef[StreamingStarted]) extends Message
  case class Initialized() extends Message
  case class AccountDataChannelInitialized() extends Message
  case class WalletUpdateTrigger() extends Message
  case class PioneerOrderSucceeded() extends Message
  case class PioneerOrderFailed(e: Throwable) extends Message
  case class DataHouseKeeping() extends Message
  case class LiquidityHouseKeeping() extends Message
  case class LiquidityBalancerRunFinished() extends Message

  case class CancelOrder(ref: OrderRef, replyTo: Option[ActorRef[CancelOrderResult]]) extends Message
  case class CancelOrderResult(exchange: String,
                               tradePair: TradePair,
                               externalOrderId: String,
                               success: Boolean,
                               orderUnknown: Boolean = false,
                               text: Option[String] = None) extends Message
  case class NewLimitOrder(orderRequest: OrderRequest, replyTo: ActorRef[NewOrderAck]) extends Message
  case class NewOrderAck(exchange: String, tradePair: TradePair, externalOrderId: String, orderId: UUID) { // answer to NewLimitOrder (done by *AccountDataChannel)
    def toOrderRef: OrderRef = OrderRef(exchange, tradePair, externalOrderId)
  }
  case class ConvertValue(value: CryptoValue, targetAsset: Asset, replyTo: ActorRef[CryptoValue]) extends Message
  case class GetPriceEstimate(pair: TradePair, replyTo: ActorRef[Double]) extends Message
  case class DetermineRealisticLimit(pair: TradePair, side: TradeSide, quantity: Double, replyTo: ActorRef[Double]) extends Message
}

class Exchange(context: ActorContext[Exchange.Message],
               timers: TimerScheduler[Exchange.Message],
               exchangeName: String,
               config: Config,
               exchangeConfig: ExchangeConfig,
               initStuff: ExchangeInitStuff) extends AbstractBehavior[Exchange.Message](context) {

  import Exchange._

  private val log = LoggerFactory.getLogger(getClass)

  private case class ExchangePublicData(var ticker: Map[TradePair, Ticker],
                                        var orderBook: Map[TradePair, OrderBook],
                                        var dataAge: DataAge)

  private case class ExchangeAccountData(var wallet: Wallet,
                                         var activeOrders: Map[OrderRef, Order])


  private implicit val system: ActorSystem[UserRootGuardian.Reply] = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = system.executionContext

  private val publicData: ExchangePublicData = ExchangePublicData(Map(), Map(), DataAge(None, None, None))
  private val accountData: ExchangeAccountData = ExchangeAccountData(
    Wallet(exchangeName, exchangeConfig.doNotTouchTheseAssets, Map()),
    Map())

  private var liquidityHouseKeepingRunning: Boolean = false

  private var tradeRoom: Option[ActorRef[TradeRoom.Message]] = None
  private var liquidityBalancerRunTempActor: ActorRef[LiquidityBalancerRun.Command] = _

  private var shutdownInitiated: Boolean = false

  private var allTradePairs: Set[TradePair] = _

  private var usableTradePairs: Set[TradePair] = _ // all trade pairs we need for calculations and trades
  private var publicDataInquirer: ActorRef[PublicDataInquirer.Command] = _

  private var liquidityManager: ActorRef[LiquidityManager.Command] = _
  private var tradeSimulator: Option[ActorRef[AccountDataChannel.Command]] = None
  private var publicDataChannel: ActorRef[PublicDataChannel.Event] = _
  private var accountDataChannel: ActorRef[AccountDataChannel.Command] = _
  private var pioneerOrderRunner: ActorRef[PioneerOrderRunner.Message] = _

  def tradeSimulationMode: Boolean = config.tradeRoom.tradeSimulation

  def startPublicDataChannel(): Unit = {
    publicDataChannel = context.spawn(
      initStuff.exchangePublicDataChannelProps(config.global, exchangeConfig, usableTradePairs, context.self, publicDataInquirer),
      s"${exchangeConfig.name}-PublicDataChannel")
  }

  def initLiquidityManager(): Unit = {
    liquidityManager = context.spawn(
      LiquidityManager(
        config,
        exchangeConfig
      ), s"LiquidityManager-$exchangeName"
    )
  }

  var tickerCompletelyInitialized: Boolean = false
  var orderBookCompletelyInitialized: Boolean = false

  def onPublicDataUpdated(): Unit = {
    if (!tickerCompletelyInitialized) {
      tickerCompletelyInitialized = usableTradePairs.subsetOf(publicData.ticker.keySet)
    }
    if (!orderBookCompletelyInitialized) {
      orderBookCompletelyInitialized = usableTradePairs.subsetOf(publicData.orderBook.keySet)
    }
    if (tickerCompletelyInitialized && orderBookCompletelyInitialized) {
        publicDataChannelInitialized.arrived()
    }
  }

  def checkIfBalanceIsSufficientForTrading(): Unit = {

    def balanceSufficient: Boolean =
      accountData.wallet.balance.contains(exchangeConfig.usdEquivalentCoin) &&
        accountData.wallet.balance(exchangeConfig.usdEquivalentCoin).amountAvailable >= config.tradeRoom.pioneerOrderValueUSD &&
        accountData.wallet.liquidCryptoValueSum(exchangeConfig.usdEquivalentCoin, publicData.ticker).amount >=
          config.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD

    val minRequiredBalance: Double = config.liquidityManager.minimumKeepReserveLiquidityPerAssetInUSD

    if (!balanceSufficient) {
      throw new RuntimeException(s"Insufficient balance for trading on $exchangeName. " +
        s"Expectation is to have at least ${config.tradeRoom.pioneerOrderValueUSD} ${exchangeConfig.usdEquivalentCoin.officialSymbol} " +
        s"for the pioneer order " +
        s"and a cumulated amount of at least $minRequiredBalance USD available for trading. " +
        s"Wallet:\n${accountData.wallet.balance.values.filter(_.amountAvailable > 0.0).mkString("\n")}")
    }
  }


  def startAccountDataManager(): Unit = {
    if (shutdownInitiated) return
    if (exchangeConfig.secrets.apiKey.isEmpty || exchangeConfig.secrets.apiSecretKey.isEmpty) {
      throw new RuntimeException(s"Can not start AccountDataManager for exchange $exchangeName because API-Key is not set")
    } else {
      accountDataChannel = context.spawn(
        initStuff.exchangeAccountDataChannelProps(config, exchangeConfig, context.self, publicDataInquirer),
        s"${exchangeConfig.name}.AccountDataChannel")

      if (tradeSimulationMode) {
        tradeSimulator = Some(context.spawn(
          TradeSimulator(config.global, exchangeConfig, context.self),
          s"TradeSimulator-$exchangeName"))
      }
    }
  }

  def initiatePioneerOrders(): Unit = {
    pioneerOrderRunner = context.spawn(
      PioneerOrderRunner(
        config,
        exchangeConfig,
        context.self),
      s"PioneerOrderRunner-$exchangeName")
  }

  def joinTradeRoom(j: JoinTradeRoom): Unit = {
    this.tradeRoom = Some(j.tradeRoom)
    initLiquidityManager()
    joinedTradeRoom.arrived()
  }

  def switchToInitializedMode(): Unit = {
    timers.startTimerAtFixedRate(DataHouseKeeping(), 1.minute)
    Future {
      concurrent.blocking {
        Thread.sleep(90000) // delay start
        timers.startTimerWithFixedDelay(LiquidityHouseKeeping(), 30.seconds)
      }
    }
    log.info(s"${Emoji.Excited}  [$exchangeName] completely initialized and running")

    context.self ! Initialized()
  }

  val accountDataChannelInitialized: WaitingFor = WaitingFor()
  val publicDataChannelInitialized: WaitingFor = WaitingFor()
  val walletInitialized: WaitingFor = WaitingFor()
  val pioneerOrdersSucceeded: WaitingFor = WaitingFor()
  val joinedTradeRoom: WaitingFor = WaitingFor()

  def startStreaming(replyTo: ActorRef[TradeRoomInitCoordinator.StreamingStarted]): Unit = {
    val maxWaitTime = config.global.internalCommunicationTimeoutDuringInit.duration
    val pioneerOrderMaxWaitTime: FiniteDuration = maxWaitTime.plus(FiniteDuration(config.tradeRoom.maxOrderLifetime.toMillis, TimeUnit.MILLISECONDS))
    val self = context.self
    val initSequence = new InitSequence(
      log,
      exchangeName,
      List(
        InitStep("start account-data-manager", () => self ! StartAccountDataManager()),
        InitStep("start public-data-channel", () => self ! StartPublicDataChannel()),
        InitStep("wait until account-data-channel initialized", () => accountDataChannelInitialized.await(maxWaitTime)),
        InitStep("wait until wallet data arrives", () => { // wait another 2 seconds for all wallet entries to arrive (bitfinex)
          walletInitialized.await(maxWaitTime)
          Thread.sleep(2000)
        }),
        InitStep("wait until public-data-channel initialized", () => publicDataChannelInitialized.await(maxWaitTime)),
        InitStep("check if balance is sufficient for trading", () => checkIfBalanceIsSufficientForTrading()),
        InitStep("warmup channels for 3 seconds", () => Thread.sleep(3000)),
        InitStep(s"initiate pioneers", () => self ! InitiatePioneerOrders()),
        InitStep("waiting for pioneer orders to succeed", () => pioneerOrdersSucceeded.await(pioneerOrderMaxWaitTime)),
        InitStep("send streaming-started", () => replyTo ! StreamingStarted(exchangeName)),
        InitStep("wait until joined trade-room", () => joinedTradeRoom.await(maxWaitTime * 3))
      ))

    Future(initSequence.run()).onComplete {
      case Success(_) => switchToInitializedMode()
      case Failure(e) =>
        val msg = s"[$exchangeName] Init sequence failed"
        throw new RuntimeException(msg, e)
    }
  }

  def setUsableTradePairs(tradePairs: Set[TradePair]): Unit = {
    usableTradePairs = tradePairs
    log.info(s"""[$exchangeName] usable trade pairs: ${tradePairs.toSeq.sortBy(_.toString).mkString(", ")}""")
  }


  def init(): Unit = {
    log.info(s"Initializing Exchange $exchangeName" +
      s"${if (tradeSimulationMode) " in TRADE-SIMULATION mode" else ""}" +
      s"${if (exchangeConfig.doNotTouchTheseAssets.nonEmpty) s" (DoNotTouch: ${exchangeConfig.doNotTouchTheseAssets.mkString(", ")})" else ""}")

    publicDataInquirer = context.spawn(
      initStuff.exchangePublicDataInquirerProps(config.global, exchangeConfig),
      s"PublicDataInquirer-$exchangeName")

    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    allTradePairs = Await.result(
      publicDataInquirer.ask(ref => PublicDataInquirer.GetAllTradePairs(ref)),
      timeout.duration.plus(500.millis)
    )
    log.info(s"""[$exchangeName] All trade pairs: ${allTradePairs.toSeq.sorted.mkString(", ")}""")
  }

  override def onMessage(message: Message): Behavior[Message] = message match {
    // @formatter:off
    case StartAccountDataManager()                => startAccountDataManager(); Behaviors.same
    case StartPublicDataChannel()                 => startPublicDataChannel(); Behaviors.same
    case InitiatePioneerOrders()                  => initiatePioneerOrders(); Behaviors.same

    case IncomingPublicData(data)                 => applyPublicDataset(data); Behaviors.same
    case IncomingAccountData(data)                => data.foreach(applyAccountData); Behaviors.same
    case SimulatedAccountData(dataset)            => applySimulatedAccountData(dataset); Behaviors.same

    case o: NewLimitOrder                         => onNewLimitOrder(o); Behaviors.same
    case c: CancelOrder                           => onCancelOrder(c); Behaviors.same // needed for pioneer order runner

    case WalletUpdateTrigger()                    => if (!walletInitialized.isArrived) walletInitialized.arrived(); Behaviors.same

    case SetUsableTradePairs(tradePairs, replyTo) =>
      setUsableTradePairs(tradePairs)
      replyTo ! TradeRoomInitCoordinator.UsableTradePairsSet(exchangeName)
      Behaviors.same

    case RemoveActiveOrder(ref)                   => accountData.activeOrders = accountData.activeOrders - ref; Behaviors.same
    case StartStreaming(replyTo)                  => startStreaming(replyTo); Behaviors.same
    case AccountDataChannelInitialized()          => accountDataChannelInitialized.arrived(); Behaviors.same
    case PioneerOrderSucceeded()                  => pioneerOrdersSucceeded.arrived(); Behaviors.same
    case PioneerOrderFailed(e)                    => log.error(s"[$exchangeName] Pioneer order failed", e); stop()

    case GetAllTradePairs(replyTo)                => replyTo ! TradeRoomInitCoordinator.AllTradePairs(exchangeName, allTradePairs); Behaviors.same
    case GetActiveOrders(replyTo)                 => replyTo ! accountData.activeOrders; Behaviors.same
    case GetPriceEstimate(tradePair, replyTo)     => replyTo ! publicData.ticker(tradePair).priceEstimate; Behaviors.same
    case GetWalletLiquidCrypto(replyTo)           =>
      replyTo ! accountData.wallet.liquidCryptoValues(exchangeConfig.usdEquivalentCoin, publicData.ticker)
      Behaviors.same

    case GetTickerSnapshot(replyTo)               => replyTo ! TickerSnapshot(exchangeName, publicData.ticker); Behaviors.same
    case ConvertValue(value, target, replyTo)     => replyTo ! value.convertTo(target, publicData.ticker); Behaviors.same

    case DetermineRealisticLimit(pair, side, quantity, replyTo) =>
      replyTo ! determineRealisticLimit(pair, side, quantity)
      Behaviors.same

    case j: JoinTradeRoom                         => joinTradeRoom(j); Behaviors.same
    case Initialized()                            => tradeRoom.get ! TradeRoomJoined(exchangeName); Behaviors.receiveMessage(initializedModeReceive)
    // @formatter:on
  }

  def checkValidity(o: OrderRequest): Unit = {
    if (exchangeConfig.doNotTouchTheseAssets.contains(o.pair.baseAsset)
      || exchangeConfig.doNotTouchTheseAssets.contains(o.pair.quoteAsset))
      throw new IllegalArgumentException("Order with DO-NOT-TOUCH asset")
  }

  def onNewLimitOrder(o: NewLimitOrder): Unit = {
    if (shutdownInitiated) return
    checkValidity(o.orderRequest)
    val forwardMsg = AccountDataChannel.NewLimitOrder(o.orderRequest, o.replyTo)
    if (tradeSimulationMode) tradeSimulator.get ! forwardMsg
    else accountDataChannel ! forwardMsg
  }

  // TODO remove order from accountData.activeOrders when order does not exist on exchange
  //       case Success(result) if result.orderUnknown =>
  //          accountData.activeOrders = accountData.activeOrders - c.ref // when order did not exist on exchange, we remove it here too
  def onCancelOrder(c: CancelOrder): Unit = {
    if (accountData.activeOrders.contains(c.ref)) {
      val forwardMsg = AccountDataChannel.CancelOrder(c.ref, c.replyTo)
      if (tradeSimulationMode) tradeSimulator.get ! forwardMsg
      else accountDataChannel ! forwardMsg
    } else {
      log.debug(s"[$exchangeName] onCancelOrder($c): order already gone")
    }
  }

  def guardedRetry(e: ExchangePublicStreamData): Unit = {
    if (e.applyDeadline.isEmpty) e.applyDeadline = Some(Instant.now.plus(e.MaxApplyDelay))
    if (Instant.now.isAfter(e.applyDeadline.get)) {
      log.warn(s"ignoring update [timeout] $e")
    } else {
      log.debug(s"scheduling retry of $e")
      context.pipeToSelf(
        Future {
          concurrent.blocking {
            Thread.sleep(20)
            IncomingPublicData(Seq(e))
          }
        })(_)
    }
  }

  private def applyPublicDataset(data: Seq[ExchangePublicStreamData]): Unit = data.foreach {
    case h: Heartbeat =>
      publicData.dataAge = publicData.dataAge.withHeartBeatTS(h.ts)

    case t: Ticker =>
      publicData.ticker = publicData.ticker.updated(t.pair, t)
      publicData.dataAge = publicData.dataAge.withTickerTS(Instant.now)
      onPublicDataUpdated()

    case b: OrderBook =>
      publicData.orderBook = publicData.orderBook.updated(b.pair, b)
      publicData.dataAge = publicData.dataAge.withOrderBookTS(Instant.now)
      onPublicDataUpdated()

    case b: OrderBookUpdate =>
      val book: Option[OrderBook] = publicData.orderBook.get(b.pair)
      if (book.isEmpty) {
        guardedRetry(b)
      } else {
        if (book.get.exchange != b.exchange) throw new IllegalArgumentException
        val newBids: Map[Double, Bid] =
          (book.get.bids -- b.bidUpdates.filter(_.quantity == 0.0).map(_.price)) ++ b.bidUpdates.filter(_.quantity != 0.0).map(e => e.price -> e)
        val newAsks: Map[Double, Ask] =
          (book.get.asks -- b.askUpdates.filter(_.quantity == 0.0).map(_.price)) ++ b.askUpdates.filter(_.quantity != 0.0).map(e => e.price -> e)

        publicData.orderBook = publicData.orderBook.updated(b.pair, OrderBook(book.get.exchange, book.get.pair, newBids, newAsks))
        publicData.dataAge = publicData.dataAge.withOrderBookTS(Instant.now)
      }
  }

  private def guardedRetry(e: ExchangeAccountStreamData): Unit = {
    if (e.applyDeadline.isEmpty) e.applyDeadline = Some(Instant.now.plus(e.MaxApplyDelay))
    if (Instant.now.isAfter(e.applyDeadline.get)) {
      log.debug(s"ignoring update [timeout] $e")
    } else {
      log.debug(s"scheduling retry of $e")
      context.pipeToSelf(
        Future(concurrent.blocking {
          Thread.sleep(20)
          IncomingAccountData(Seq(e))
        }))(_)
    }
  }

  private def applyAccountData(data: ExchangeAccountStreamData): Unit = {
    if (log.isTraceEnabled) log.trace(s"applying incoming $data")

    data match {
      case w: WalletBalanceUpdate =>
        val balance = accountData.wallet.balance.map {
          case (a: Asset, b: Balance) if a == w.asset =>
            (a, Balance(b.asset, b.amountAvailable + w.amountDelta, b.amountLocked))
          case (a: Asset, b: Balance) => (a, b)
        }.filterNot(e => e._2.amountAvailable == 0.0 && e._2.amountLocked == 0.0)
          .filterNot(e => exchangeConfig.assetBlocklist.contains(e._1))
        accountData.wallet = accountData.wallet.withBalance(balance)
        context.self ! WalletUpdateTrigger()

      case w: WalletAssetUpdate =>
        val balance = (accountData.wallet.balance ++ w.balance)
          .filterNot(e => e._2.amountAvailable == 0.0 && e._2.amountLocked == 0.0)
          .filterNot(e => exchangeConfig.assetBlocklist.contains(e._1))
        accountData.wallet = accountData.wallet.withBalance(balance)
        context.self ! WalletUpdateTrigger()

      case w: CompleteWalletUpdate =>
        val balance = w.balance
          .filterNot(e => e._2.amountAvailable == 0.0 && e._2.amountLocked == 0.0)
          .filterNot(e => exchangeConfig.assetBlocklist.contains(e._1))
        if (balance != accountData.wallet.balance) { // we get snapshots delivered here, so updates are needed only, when something changed
          accountData.wallet = accountData.wallet.withBalance(balance)
          context.self ! WalletUpdateTrigger()
        }

      case o: OrderUpdate =>
        val ref = OrderRef(exchangeConfig.name, o.pair, o.externalOrderId)
        if (accountData.activeOrders.contains(ref)) {
          accountData.activeOrders(ref).applyUpdate(o)
        } else {
          if (o.orderType.isDefined) {
            accountData.activeOrders = accountData.activeOrders.updated(ref, o.toOrder)
          } else {
            // better wait for initial order update before applying this one
            guardedRetry(o)
          }
        }

        if (tradeRoom.isDefined) tradeRoom.get ! OrderUpdateTrigger(ref)

      case _ => throw new NotImplementedError
    }
  }

  def applySimulatedAccountData(dataset: ExchangeAccountStreamData): Unit = {
    if (!config.tradeRoom.tradeSimulation) throw new RuntimeException
    log.trace(s"Applying simulation data ...")
    applyAccountData(dataset)
  }

  /**
   * Will trigger a restart of the TradeRoom if stale data is found
   */
  def checkIfWeHaveStalePublicData(): Boolean = {
    val lastSeen: Instant = publicData.dataAge.latest
    if (Duration.between(lastSeen, Instant.now).compareTo(config.tradeRoom.restarWhenDataStreamIsOlderThan) > 0) {
      val msg = s"[$exchangeName] public data is outdated!"
      log.error(msg) // TODO check if we need to log here too. Better only the exception get caught and logged
      throw new StaleDataException(msg)
    } else false
  }

  def dataHouseKeeping(): Unit = {
    checkIfWeHaveStalePublicData()
  }

  def liquidityHouseKeeping(): Unit = {
    if (liquidityHouseKeepingRunning) return
    liquidityHouseKeepingRunning = true

    implicit val timeout: Timeout = config.global.internalCommunicationTimeout

    val f1 = liquidityManager.ask(ref => LiquidityManager.GetState(ref))
    val f2 = tradeRoom.get.ask(ref => TradeRoom.GetReferenceTicker(ref))
    Await.result(for {
      lmState <- f1
      referenceTicker <- f2
    } yield (lmState, referenceTicker.ticker),
      config.global.internalCommunicationTimeout.duration
    ) match {
      case (lmState, referenceTicker) =>
        val wc = WorkingContext(
          publicData.ticker,
          referenceTicker,
          publicData.orderBook,
          accountData.wallet.balance.map(e => e._1 -> e._2),
          lmState.liquidityDemand,
          lmState.liquidityLocks)

        liquidityBalancerRunTempActor = context.spawn(
          LiquidityBalancerRun(config, exchangeConfig, usableTradePairs, context.self, tradeRoom.get, wc),
          s"${exchangeConfig.name}-liquidity-balancer-run")
    }
  }

  def stop(): Behavior[Message] = {
    shutdownInitiated = true
    Behaviors.stopped
  }

  def removeOrphanOrder(ref: OrderRef): Unit = {
    val order = accountData.activeOrders.get(ref)
    accountData.activeOrders = accountData.activeOrders - ref
    if (order.isDefined) {
      log.info(s"[$exchangeName] cleaned up orphan finished order ${
        order.get
      }")
    }
  }

  def determineRealisticLimit(pair: TradePair, side: TradeSide, quantity: Double): Double = {
    exchange.determineRealisticLimit(
      pair,
      side,
      quantity,
      config.liquidityManager.orderbookBasedTxLimitQuantityOverbooking,
      config.liquidityManager.tickerBasedTxLimitBeyondEdgeLimit,
      publicData.ticker,
      publicData.orderBook)
  }

  def exchangeDataSnapshot: TradeRoom.FullDataSnapshot =
    TradeRoom.FullDataSnapshot(
      exchangeName,
      publicData.ticker,
      publicData.orderBook,
      publicData.dataAge,
      accountData.wallet
    )


  def initializedModeReceive(message: Message): Behavior[Message] = {
    message match {
      // @formatter:off
      case IncomingPublicData(data)        => applyPublicDataset(data)
      case IncomingAccountData(data)       => data.foreach(applyAccountData)
      case SimulatedAccountData(dataset)   => applySimulatedAccountData(dataset)

      case _: WalletUpdateTrigger          => // currently unused

      case m: NewLimitOrder                => onNewLimitOrder(m)
      case m: CancelOrder                  => onCancelOrder(m)
      case m: LiquidityLockRequest         => liquidityManager ! m.withWallet(accountData.wallet)
      case m: LiquidityLockClearance       => liquidityManager ! m
      case RemoveActiveOrder(ref)          => accountData.activeOrders = accountData.activeOrders - ref
      case RemoveOrphanOrder(ref)          => removeOrphanOrder(ref)

      case GetTickerSnapshot(replyTo)      => replyTo ! TickerSnapshot(exchangeName, publicData.ticker)
      case GetWallet(replyTo)              => replyTo ! accountData.wallet
      case GetActiveOrders(replyTo)        => replyTo ! accountData.activeOrders
      case GetFullDataSnapshot(replyTo)    => replyTo ! exchangeDataSnapshot

      case AccountDataChannelInitialized() => log.info(s"[$exchangeName] account data channel re-initialized")

      case DataHouseKeeping()              => dataHouseKeeping()
      case LiquidityHouseKeeping()         => liquidityHouseKeeping()
      case LiquidityBalancerRunFinished()  => liquidityHouseKeepingRunning = false
      // @formatter:on
    }

    Behaviors.same
  }

  init()


  //  override val supervisorStrategy: OneForOneStrategy = {
  //    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 5.minutes, loggingEnabled = true) {
  //      // @formatter:off
//      case _: ActorKilledException => Restart
//      case _: Exception            => Escalate
//      // @formatter:on
  //    }
  //  }
}
