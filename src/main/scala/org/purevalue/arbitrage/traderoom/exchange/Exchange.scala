package org.purevalue.arbitrage.traderoom.exchange

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorKilledException, ActorLogging, ActorRef, ActorSystem, Cancellable, Kill, OneForOneStrategy, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.traderoom.TradeRoom.{GetReferenceTicker, JoinTradeRoom, OrderRef, TradeRoomJoined}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.LiquidityBalancer.WorkingContext
import org.purevalue.arbitrage.traderoom.exchange.LiquidityManager.{GetState, LiquidityLockClearance, LiquidityLockRequest}
import org.purevalue.arbitrage.traderoom.exchange.PioneerOrderRunner.{PioneerOrderFailed, PioneerOrderSucceeded}
import org.purevalue.arbitrage.util.{Emoji, InitSequence, InitStep, WaitingFor}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

case class TickerSnapshot(exchange: String, ticker: Map[TradePair, Ticker])
case class OrderBookSnapshot(exchange: String, orderBook: Map[TradePair, OrderBook])
case class DataTSSnapshot(exchange: String, heartbeatTS: Option[Instant], tickerTS: Option[Instant], orderBookTS: Option[Instant])
case class FullDataSnapshot(exchange: String,
                            ticker: Map[TradePair, Ticker],
                            orderBook: Map[TradePair, OrderBook],
                            heartbeatTS: Option[Instant],
                            tickerTS: Option[Instant],
                            orderBookTS: Option[Instant],
                            wallet: Wallet)

object Exchange {
  case class IncomingPublicData(data: Seq[ExchangePublicStreamData])
  case class IncomingAccountData(data: Seq[ExchangeAccountStreamData])

  case class GetAllTradePairs()
  case class GetTickerSnapshot()
  case class GetWallet()
  case class GetFullDataSnapshot()
  case class GetActiveOrder(r: OrderRef)
  case class GetActiveOrders()
  case class GetWalletLiquidCrypto()

  case class SetUsableTradePairs(tradeable: Set[TradePair])
  case class RemoveActiveOrder(ref: OrderRef)
  case class RemoveOrphanOrder(ref: OrderRef)

  case class StartStreaming()
  case class StreamingStarted(exchange: String)
  case class AccountDataChannelInitialized()
  case class WalletUpdateTrigger()
  case class OrderUpdateTrigger(ref: OrderRef, resendCounter: Int = 0) // status of an order has changed
  case class DataHouseKeeping()
  case class LiquidityHouseKeeping()
  case class SwitchToInitializedMode()

  case class CancelOrder(ref: OrderRef)
  case class CancelOrderResult(exchange: String,
                               tradePair: TradePair,
                               externalOrderId: String,
                               success: Boolean,
                               orderUnknown: Boolean = false,
                               text: Option[String] = None)
  case class NewLimitOrder(orderRequest: OrderRequest) // response is NewOrderAck
  case class NewOrderAck(exchange: String, tradePair: TradePair, externalOrderId: String, orderId: UUID) {
    def toOrderRef: OrderRef = OrderRef(exchange, tradePair, externalOrderId)
  }

  case class ConvertValue(value: CryptoValue, targetAsset: Asset) // return CryptoValue
  case class GetPriceEstimate(tradepair: TradePair) // return Double
  case class DetermineRealisticLimit(pair: TradePair, side: TradeSide, quantity: Double, realityAdjustmentRate: Double) // return Double

  def props(exchangeName: String,
            config: Config,
            exchangeConfig: ExchangeConfig,
            initStuff: ExchangeInitStuff): Props =
    Props(new Exchange(exchangeName, config, exchangeConfig, initStuff))
}

case class Exchange(exchangeName: String,
                    config: Config,
                    exchangeConfig: ExchangeConfig,
                    initStuff: ExchangeInitStuff) extends Actor with ActorLogging {

  private case class ExchangePublicData(var ticker: Map[TradePair, Ticker],
                                        var orderBook: Map[TradePair, OrderBook],
                                        var heartbeatTS: Option[Instant],
                                        var tickerTS: Option[Instant],
                                        var orderBookTS: Option[Instant])

  private case class ExchangeAccountData(var wallet: Wallet,
                                         var activeOrders: Map[OrderRef, Order])


  private implicit val actorSystem: ActorSystem = Main.actorSystem
  private implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  private val publicData: ExchangePublicData = ExchangePublicData(Map(), Map(), None, None, None)
  private val accountData: ExchangeAccountData = ExchangeAccountData(
    Wallet(exchangeName, exchangeConfig.doNotTouchTheseAssets, Map()),
    Map())

  private var dataHouseKeepingSchedule: Cancellable = _
  private var liquidityHouseKeepingSchedule: Cancellable = _
  private var liquidityHouseKeepingRunning: Boolean = false

  private var tradeRoom: Option[ActorRef] = None
  private var liquidityBalancerRunTempActor: ActorRef = _

  private var shutdownInitiated: Boolean = false

  private var allTradePairs: Set[TradePair] = _

  private var usableTradePairs: Set[TradePair] = _ // all trade pairs we need for calculations and trades
  private var publicDataInquirer: ActorRef = _

  private var liquidityManager: ActorRef = _
  private var tradeSimulator: Option[ActorRef] = None
  private var publicDataChannel: ActorRef = _
  private var accountDataChannel: ActorRef = _
  private var pioneerOrderRunner: ActorRef = _

  def tradeSimulationMode: Boolean = config.tradeRoom.tradeSimulation

  def startPublicDataChannel(): Unit = {
    publicDataChannel = context.actorOf(
      initStuff.exchangePublicDataChannelProps(config.global, exchangeConfig, usableTradePairs, self, publicDataInquirer),
      s"${exchangeConfig.name}-PublicDataChannel")
  }

  def initLiquidityManager(j: JoinTradeRoom): Unit = {
    liquidityManager = context.actorOf(
      LiquidityManager.props(
        config,
        exchangeConfig,
        usableTradePairs,
        accountData.wallet,
        j.tradeRoom),
      s"LiquidityManager-$exchangeName"
    )
  }

  var tickerCompletelyInitialized: Boolean = false

  def onTickerUpdate(): Unit = {
    if (!tickerCompletelyInitialized) {
      tickerCompletelyInitialized = usableTradePairs.subsetOf(publicData.ticker.keySet)
      if (tickerCompletelyInitialized) {
        publicDataChannelInitialized.arrived()
      }
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

  def startPublicDataInquirer(): Unit = {
    publicDataInquirer = context.actorOf(initStuff.exchangePublicDataInquirerProps(config.global, exchangeConfig), s"PublicDataInquirer-$exchangeName")

    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    allTradePairs = Await.result(
      (publicDataInquirer ? GetAllTradePairs()).mapTo[Set[TradePair]],
      timeout.duration.plus(500.millis))

    log.debug(s"$exchangeName: ${allTradePairs.size} Total TradePairs: ${allTradePairs.toSeq.sortBy(e => e.toString)}")
  }

  def startAccountDataManager(): Unit = {
    if (shutdownInitiated) return
    if (exchangeConfig.secrets.apiKey.isEmpty || exchangeConfig.secrets.apiSecretKey.isEmpty) {
      throw new RuntimeException(s"Can not start AccountDataManager for exchange $exchangeName because API-Key is not set")
    } else {
      accountDataChannel = context.actorOf(initStuff.exchangeAccountDataChannelProps(config, exchangeConfig, self, publicDataInquirer),
        s"${exchangeConfig.name}.AccountDataChannel")

      if (tradeSimulationMode) {
        tradeSimulator = Some(context.actorOf(TradeSimulator.props(config.global, exchangeConfig, self), s"TradeSimulator-$exchangeName"))
      }
    }
  }

  def initiatePioneerOrders(): Unit = {
    pioneerOrderRunner = context.actorOf(
      PioneerOrderRunner.props(
        config,
        exchangeConfig,
        self),
      s"PioneerOrderRunner-$exchangeName")
  }

  def joinTradeRoom(j: JoinTradeRoom): Unit = {
    this.tradeRoom = Some(j.tradeRoom)
    initLiquidityManager(j)
    joinedTradeRoom.arrived()
  }

  def switchToInitializedMode(): Unit = {
    context.become(initializedModeReceive)
    dataHouseKeepingSchedule = actorSystem.scheduler.scheduleAtFixedRate(20.seconds, 1.minute, self, DataHouseKeeping())
    liquidityHouseKeepingSchedule = actorSystem.scheduler.scheduleWithFixedDelay(90.seconds, 30.seconds, self, LiquidityHouseKeeping())
    log.info(s"${Emoji.Excited}  [$exchangeName] completely initialized and running")
    tradeRoom.get ! TradeRoomJoined(exchangeName)
  }

  val accountDataChannelInitialized: WaitingFor = WaitingFor()
  val publicDataChannelInitialized: WaitingFor = WaitingFor()
  val walletInitialized: WaitingFor = WaitingFor()
  val pioneerOrdersSucceeded: WaitingFor = WaitingFor()
  val joinedTradeRoom: WaitingFor = WaitingFor()

  def startStreaming(): Unit = {
    val sendStreamingStartedResponseTo = sender()
    val maxWaitTime = config.global.internalCommunicationTimeoutDuringInit.duration
    val pioneerOrderMaxWaitTime: FiniteDuration = maxWaitTime.plus(FiniteDuration(config.tradeRoom.maxOrderLifetime.toMillis, TimeUnit.MILLISECONDS))
    val initSequence = new InitSequence(
      log,
      exchangeName,
      List(
        InitStep("start account-data-manager", () => startAccountDataManager()),
        InitStep("start public-data-channel", () => startPublicDataChannel()),
        InitStep("wait until account-data-channel initialized", () => accountDataChannelInitialized.await(maxWaitTime)),
        InitStep("wait until wallet data arrives", () => { // wait another 2 seconds for all wallet entries to arrive (bitfinex)
          walletInitialized.await(maxWaitTime)
          Thread.sleep(2000)
        }),
        InitStep("wait until public-data-channel initialized", () => publicDataChannelInitialized.await(maxWaitTime)),
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
        log.error(e, s"[$exchangeName] Init sequence failed")
        self ! PoisonPill // TODO coordinated shutdown
    }
  }

  def setUsableTradePairs(tradePairs: Set[TradePair]): Unit = {
    usableTradePairs = tradePairs
    log.info(s"""[$exchangeName] usable trade pairs: ${tradePairs.toSeq.sortBy(_.toString).mkString(", ")}""")
    sender() ! Done
  }

  override def preStart(): Unit = {
    try {
      log.info(s"Initializing Exchange $exchangeName" +
        s"${if (tradeSimulationMode) " in TRADE-SIMULATION mode" else ""}" +
        s"${if (exchangeConfig.doNotTouchTheseAssets.nonEmpty) s" (DoNotTouch: ${exchangeConfig.doNotTouchTheseAssets.mkString(", ")})" else ""}")

      startPublicDataInquirer()

    } catch {
      case e: Exception => log.error(e, s"$exchangeName: preStart failed")
      // TODO coordinated shutdown
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case IncomingPublicData(data)                 => applyPublicDataset(data)
    case IncomingAccountData(data)                => data.foreach(applyAccountData)
    case SimulatedAccountData(dataset)            => applySimulatedAccountData(dataset)

    case o: NewLimitOrder                         => onNewLimitOrder(o) // needed for pioneer order runner
    case c: CancelOrder                           => onCancelOrder(c) // needed for pioneer order runner

    case WalletUpdateTrigger()                    => if (!walletInitialized.isArrived) walletInitialized.arrived()
    case t: OrderUpdateTrigger                    => if (tradeRoom.isDefined) tradeRoom.get.forward(t)

    case SetUsableTradePairs(tradePairs)          => setUsableTradePairs(tradePairs)
    case RemoveActiveOrder(ref)                   => accountData.activeOrders = accountData.activeOrders - ref
    case StartStreaming()                         => startStreaming()
    case AccountDataChannelInitialized()          => accountDataChannelInitialized.arrived()
    case PioneerOrderSucceeded()                  => pioneerOrdersSucceeded.arrived()
    case PioneerOrderFailed(e)                    => log.error(e, s"[$exchangeName] Pioneer order failed"); self ! PoisonPill
    case j: JoinTradeRoom                         => joinTradeRoom(j)

    case GetAllTradePairs()                       => sender() ! allTradePairs
    case GetActiveOrders()                        => sender() ! accountData.activeOrders
    case GetPriceEstimate(tradePair)              => sender() ! publicData.ticker(tradePair).priceEstimate
    case GetWalletLiquidCrypto()                  => sender() ! accountData.wallet.liquidCryptoValues(exchangeConfig.usdEquivalentCoin, publicData.ticker)
    case ConvertValue(value, target)              => sender() ! value.convertTo(target, publicData.ticker)
    case m:DetermineRealisticLimit                => determineRealisticLimit(m).pipeTo(sender())

    case Done                                     => // ignoring Done from cascaded JoinTradeRoom
    case SwitchToInitializedMode()                => switchToInitializedMode()
    case s: TradeRoom.Stop                        => onStop(s)
  }
  // @formatter:on

  def checkValidity(o: OrderRequest): Unit = {
    if (exchangeConfig.doNotTouchTheseAssets.contains(o.pair.baseAsset)
      || exchangeConfig.doNotTouchTheseAssets.contains(o.pair.quoteAsset))
      throw new IllegalArgumentException("Order with DO-NOT-TOUCH asset")
  }

  def onNewLimitOrder(o: NewLimitOrder): Unit = {
    if (shutdownInitiated) return

    checkValidity(o.orderRequest)
    if (tradeSimulationMode) tradeSimulator.get.forward(o)
    else accountDataChannel.forward(o)
  }

  def cancelOrderIfStillExist(c: CancelOrder): Unit = {
    val cancelOrderOrigin = sender()
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    if (accountData.activeOrders.contains(c.ref)) {
      (accountDataChannel ? c).mapTo[CancelOrderResult].onComplete {
        case Success(result) if result.orderUnknown =>
          accountData.activeOrders = accountData.activeOrders - c.ref // when order did not exist on exchange, we remove it here too
          cancelOrderOrigin ! result
          log.debug(s"removed order ${c.ref} in activeOrders")
        case Success(result) => cancelOrderOrigin ! result
        case Failure(e) =>
          log.error(e, "[houston, we...] CancelOrder failed")
      }
    }
  }

  def onCancelOrder(c: CancelOrder): Unit = {
    if (tradeSimulationMode) tradeSimulator.get.forward(c)
    else cancelOrderIfStillExist(c)
  }


  def guardedRetry(e: ExchangePublicStreamData): Unit = {
    if (e.applyDeadline.isEmpty) e.applyDeadline = Some(Instant.now.plus(e.MaxApplyDelay))
    if (Instant.now.isAfter(e.applyDeadline.get)) {
      log.warning(s"ignoring update [timeout] $e")
    } else {
      log.debug(s"scheduling retry of $e")
      Future( concurrent.blocking {
        Thread.sleep(20)
        self ! IncomingPublicData(Seq(e))
      })
    }
  }

  private def applyPublicDataset(data: Seq[ExchangePublicStreamData]): Unit = {
    data.foreach {
      case h: Heartbeat =>
        publicData.heartbeatTS = Some(h.ts)

      case t: Ticker =>
        publicData.ticker = publicData.ticker.updated(t.tradePair, t)
        publicData.tickerTS = Some(Instant.now)
        onTickerUpdate()

      case b: OrderBook =>
        publicData.orderBook = publicData.orderBook.updated(b.tradePair, b)
        publicData.orderBookTS = Some(Instant.now)

      case b: OrderBookUpdate =>
        val book: Option[OrderBook] = publicData.orderBook.get(b.tradePair)
        if (book.isEmpty) {
          guardedRetry(b)
        } else {
          if (book.get.exchange != b.exchange) throw new IllegalArgumentException
          val newBids: Map[Double, Bid] =
            (book.get.bids -- b.bidUpdates.filter(_.quantity == 0.0).map(_.price)) ++ b.bidUpdates.filter(_.quantity != 0.0).map(e => e.price -> e)
          val newAsks: Map[Double, Ask] =
            (book.get.asks -- b.askUpdates.filter(_.quantity == 0.0).map(_.price)) ++ b.askUpdates.filter(_.quantity != 0.0).map(e => e.price -> e)

          publicData.orderBook = publicData.orderBook.updated(b.tradePair, OrderBook(book.get.exchange, book.get.tradePair, newBids, newAsks))
          publicData.orderBookTS = Some(Instant.now)
        }
    }
  }

  private def guardedRetry(e: ExchangeAccountStreamData): Unit = {
    if (e.applyDeadline.isEmpty) e.applyDeadline = Some(Instant.now.plus(e.MaxApplyDelay))
    if (Instant.now.isAfter(e.applyDeadline.get)) {
      log.debug(s"ignoring update [timeout] $e")
    } else {
      log.debug(s"scheduling retry of $e")
      Future( concurrent.blocking {
        Thread.sleep(20)
        self ! IncomingAccountData(Seq(e))
      })
    }
  }

  private def applyAccountData(data: ExchangeAccountStreamData): Unit = {
    if (log.isDebugEnabled) log.debug(s"applying incoming $data")

    data match {
      case w: WalletBalanceUpdate =>
        val balance = accountData.wallet.balance.map {
          case (a: Asset, b: Balance) if a == w.asset =>
            (a, Balance(b.asset, b.amountAvailable + w.amountDelta, b.amountLocked))
          case (a: Asset, b: Balance) => (a, b)
        }.filterNot(e => e._2.amountAvailable == 0.0 && e._2.amountLocked == 0.0)
          .filterNot(e => exchangeConfig.assetBlocklist.contains(e._1))
        accountData.wallet = accountData.wallet.withBalance(balance)
        self ! WalletUpdateTrigger()

      case w: WalletAssetUpdate =>
        val balance = (accountData.wallet.balance ++ w.balance)
          .filterNot(e => e._2.amountAvailable == 0.0 && e._2.amountLocked == 0.0)
          .filterNot(e => exchangeConfig.assetBlocklist.contains(e._1))
        accountData.wallet = accountData.wallet.withBalance(balance)
        self ! WalletUpdateTrigger()

      case w: CompleteWalletUpdate =>
        val balance = w.balance
          .filterNot(e => e._2.amountAvailable == 0.0 && e._2.amountLocked == 0.0)
          .filterNot(e => exchangeConfig.assetBlocklist.contains(e._1))
        if (balance != accountData.wallet.balance) { // we get snapshots delivered here, so updates are needed only, when something changed
          accountData.wallet = accountData.wallet.withBalance(balance)
          self ! WalletUpdateTrigger()
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
        self ! OrderUpdateTrigger(ref)

      case _ => throw new NotImplementedError
    }
  }

  def applySimulatedAccountData(dataset: ExchangeAccountStreamData): Unit = {
    if (!config.tradeRoom.tradeSimulation) throw new RuntimeException
    log.debug(s"Applying simulation data ...")
    applyAccountData(dataset)
  }

  /**
   * Will trigger a restart of the TradeRoom if stale data is found
   */
  def stalePublicDataWatch(): Unit = {
    val lastSeen: Instant = (publicData.heartbeatTS.toSeq ++ publicData.tickerTS.toSeq ++ publicData.orderBookTS.toSeq).max
    if (Duration.between(lastSeen, Instant.now).compareTo(config.tradeRoom.restarWhenDataStreamIsOlderThan) > 0) {
      log.error(s"[$exchangeName] data is outdated!")
      self ! Kill
    }
  }

  def dataHouseKeeping(): Unit = {
    stalePublicDataWatch()
  }

  def liquidityHouseKeeping(): Unit = {
    if (liquidityHouseKeepingRunning) return
    liquidityHouseKeepingRunning = true

    implicit val timeout: Timeout = config.global.internalCommunicationTimeout

    val f1 = (liquidityManager ? GetState()).mapTo[LiquidityManager.State]
    val f2 = (tradeRoom.get ? GetReferenceTicker()).mapTo[TickerSnapshot]
    (for {
      lmState <- f1
      referenceTicker <- f2
    } yield (lmState, referenceTicker.ticker)).onComplete {

      case Success((lmState, referenceTicker)) =>
        val wc =
          WorkingContext(
            publicData.ticker,
            referenceTicker,
            publicData.orderBook,
            accountData.wallet.balance.map(e => e._1 -> e._2),
            lmState.liquidityDemand,
            lmState.liquidityLocks)

        liquidityBalancerRunTempActor = context.actorOf(LiquidityBalancerRun.props(config, exchangeConfig, usableTradePairs, self, tradeRoom.get, wc),
          s"${exchangeConfig.name}-liquidity-balancer-run")

      case Failure(cause) => log.error(cause, s"[$exchangeName]  liquidityHouseKeeping()")
    }
  }

  def onStop(s: TradeRoom.Stop): Unit = {
    shutdownInitiated = true
    liquidityManager ! s
    self ! PoisonPill
  }

  def removeOrphanOrder(ref: OrderRef): Unit = {
    val order = accountData.activeOrders.get(ref)
    accountData.activeOrders = accountData.activeOrders - ref
    if (order.isDefined) {
      log.info(s"[$exchangeName] cleaned up orphan finished order ${order.get}")
    }
  }

  def determineRealisticLimit(r: DetermineRealisticLimit): Future[Double] = {
    Future {
      exchange.determineRealisticLimit(r.pair, r.side, r.quantity, r.realityAdjustmentRate, publicData.ticker, publicData.orderBook)
    }
  }

  def exchangeDataSnapshot: FullDataSnapshot = FullDataSnapshot(
    exchangeName,
    publicData.ticker,
    publicData.orderBook,
    publicData.heartbeatTS, publicData.tickerTS, publicData.orderBookTS,
    accountData.wallet
  )

  // @formatter:off
  def initializedModeReceive: Receive = {
    case IncomingPublicData(data)        => applyPublicDataset(data)
    case IncomingAccountData(data)       => data.foreach(applyAccountData)
    case SimulatedAccountData(dataset)   => applySimulatedAccountData(dataset)

    case _: WalletUpdateTrigger          => // currently unused
    case t: OrderUpdateTrigger           => tradeRoom.get.forward(t)

    case o: NewLimitOrder                => onNewLimitOrder(o)
    case c: CancelOrder                  => onCancelOrder(c)
    case l: LiquidityLockRequest         => liquidityManager.forward(l)
    case c: LiquidityLockClearance       => liquidityManager.forward(c)
    case RemoveActiveOrder(ref)          => accountData.activeOrders = accountData.activeOrders - ref
    case RemoveOrphanOrder(ref)          => removeOrphanOrder(ref)

    case GetTickerSnapshot()             => sender() ! TickerSnapshot(exchangeName, publicData.ticker)
    case GetWallet()                     => sender() ! accountData.wallet
    case GetActiveOrder(ref)             => sender() ! accountData.activeOrders.get(ref)
    case GetActiveOrders()               => sender() ! accountData.activeOrders
    case GetFullDataSnapshot()           => sender() ! exchangeDataSnapshot

    case GetPriceEstimate(tradePair)     => sender() ! publicData.ticker(tradePair).priceEstimate
    case m:DetermineRealisticLimit       => determineRealisticLimit(m).pipeTo(sender())
    case ConvertValue(value, target)     => sender() ! value.convertTo(target, publicData.ticker)

    case AccountDataChannelInitialized() => log.info(s"[$exchangeName] account data channel re-initialized")

    case DataHouseKeeping()              => dataHouseKeeping()
    case LiquidityHouseKeeping()         => liquidityHouseKeeping()
    case LiquidityBalancerRun.Finished() => liquidityHouseKeepingRunning = false
    case s: TradeRoom.Stop               => onStop(s)

    case Failure(cause)                  => log.error(cause, s"[$exchangeName] failure received")
  }
  // @formatter:off

  override val supervisorStrategy: OneForOneStrategy = {
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 5.minutes, loggingEnabled = true) {
      // @formatter:off
      case _: ActorKilledException => Restart
      case _: Exception            => Escalate
      // @formatter:on
    }
  }
}
