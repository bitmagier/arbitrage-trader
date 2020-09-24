package org.purevalue.arbitrage.adapter.binance

import java.time.Instant
import java.util.UUID

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{HttpMethods, StatusCode, StatusCodes, Uri}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager._
import org.purevalue.arbitrage.adapter._
import org.purevalue.arbitrage.adapter.binance.BinanceAccountDataChannel.{Connect, SendPing}
import org.purevalue.arbitrage.adapter.binance.BinanceOrder.{toOrderStatus, toOrderType, toTradeSide}
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.{BinanceBaseRestEndpoint, GetBinanceTradePairs}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.util.HttpUtil.{httpRequestJsonBinanceAccount, httpRequestPureJsonBinanceAccount}
import org.purevalue.arbitrage.util.Util.{formatDecimal, formatDecimalWithFixPrecision}
import org.purevalue.arbitrage.util.{BadCalculationError, WrongAssumption}
import org.purevalue.arbitrage.{adapter, _}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


object BinanceAccountDataChannel {
  private case class Connect()
  private case class SendPing()
  private case class QueryAccountInformation()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            exchangeAccountDataManager: ActorRef,
            publicDataInquirer: ActorRef): Props =
    Props(new BinanceAccountDataChannel(globalConfig, exchangeConfig, exchangeAccountDataManager, publicDataInquirer))
}

class BinanceAccountDataChannel(globalConfig: GlobalConfig,
                                exchangeConfig: ExchangeConfig,
                                exchangeAccountDataManager: ActorRef,
                                exchangePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceAccountDataChannel])

  // outboundAccountPosition is sent any time an account balance has changed and contains the assets
  // that were possibly changed by the event that generated the balance change
  private val OutboundAccountPositionStreamName = "outboundAccountPosition"
  private val IdOutboundAccountPositionStream: Int = 1
  // Balance Update occurs during the following:
  // - Deposits or withdrawals from the account
  // - Transfer of funds between accounts (e.g. Spot to Margin)
  private val BalanceUpdateStreamName = "balanceUpdate"
  private val IdBalanceUpdateStream: Int = 2
  // Orders are updated with the executionReport event
  private val OrderExecutionReportStreamName = "executionReport"
  private val IdOrderExecutionResportStream: Int = 3

  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var outstandingStreamSubscribeResponses: Set[Int] = Set(IdOutboundAccountPositionStream, IdBalanceUpdateStream, IdOrderExecutionResportStream)

  val pingSchedule: Cancellable = system.scheduler.scheduleAtFixedRate(30.minutes, 30.minutes, self, SendPing())

  var listenKey: String = _
  var binanceTradePairsBySymbol: Map[String, BinanceTradePair] = _
  var binanceTradePairsByTradePair: Map[TradePair, BinanceTradePair] = _

  import BinanceAccountDataJsonProtocoll._


  def exchangeDataMapping(in: Seq[IncomingBinanceAccountJson]): Seq[ExchangeAccountStreamData] = in.map {
    // @formatter:off
    case a: AccountInformationJson      => a.toWalletAssetUpdate // deprecated
    case a: OutboundAccountPositionJson => a.toWalletAssetUpdate
    case b: BalanceUpdateJson           => b.toWalletBalanceUpdate
    case o: NewOrderResponseFullJson    => o.toOrderUpdate(exchangeConfig.exchangeName, symbol => binanceTradePairsBySymbol(symbol).toTradePair) // expecting, that we have all relevant trade pairs
    case o: OrderExecutionReportJson    => o.toOrderUpdate(exchangeConfig.exchangeName, symbol => binanceTradePairsBySymbol(symbol).toTradePair)
    case o: OpenOrderJson               => o.toOrderUpdate(exchangeConfig.exchangeName, symbol => binanceTradePairsBySymbol(symbol).toTradePair)
    case x                              => log.debug(s"$x"); throw new NotImplementedError
    // @formatter:on
  }

  def streamSubscribeResponse(j: JsObject): Unit = {
    if (log.isTraceEnabled) log.trace(s"received $j")
    val channelId = j.fields("id").convertTo[Int]

    val initialized: Boolean = synchronized {
      outstandingStreamSubscribeResponses = outstandingStreamSubscribeResponses - channelId
      outstandingStreamSubscribeResponses.isEmpty
    }

    if (initialized) {
      log.debug("all streams running")
      onStreamsRunning()
    }
  }

  def decodeMessage(message: Message): Future[Seq[IncomingBinanceAccountJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject())
        .map {
          case j: JsObject if j.fields.contains("result") =>
            streamSubscribeResponse(j: JsObject)
            Nil
          case j: JsObject if j.fields.contains("e") =>
            if (log.isTraceEnabled) log.trace(s"received $j")
            j.fields("e").convertTo[String] match {
              case OutboundAccountPositionStreamName => List(j.convertTo[OutboundAccountPositionJson])
              case BalanceUpdateStreamName => List(j.convertTo[BalanceUpdateJson])
              case OrderExecutionReportStreamName => List(j.convertTo[OrderExecutionReportJson])
              case name: String =>
                log.error(s"Unknown data stream '$name' received: $j")
                Nil
            }
          case j: JsObject =>
            log.warn(s"Unknown json object received: $j")
            Nil
        }
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(Nil)
  }

  val SubscribeMessages: List[AccountStreamSubscribeRequestJson] = List(
    AccountStreamSubscribeRequestJson(params = Seq(OutboundAccountPositionStreamName), id = IdOutboundAccountPositionStream),
    AccountStreamSubscribeRequestJson(params = Seq(BalanceUpdateStreamName), id = IdBalanceUpdateStream),
    AccountStreamSubscribeRequestJson(params = Seq(OrderExecutionReportStreamName), id = IdOrderExecutionResportStream)
  )

  val wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .map(exchangeDataMapping)
          .map(IncomingData)
          .pipeTo(exchangeAccountDataManager)
      ),
      Source(
        SubscribeMessages.map(msg => TextMessage(msg.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/ws/$listenKey")
  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
  var connected: Future[Done.type] = _

  def createConnected: Future[Done.type] =
    ws._1.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        if (log.isTraceEnabled) log.trace("WebSocket connected")
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  def deliverAccountInformation(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    httpRequestJsonBinanceAccount[AccountInformationJson, JsValue](HttpMethods.GET, s"$BinanceBaseRestEndpoint/api/v3/account", None, exchangeConfig.secrets, sign = true)
      .map {
        case Left(response) => IncomingData(exchangeDataMapping(Seq(response)))
        case Right(errorResponse) => throw new RuntimeException(s"deliverAccountInformation failed: $errorResponse")
      }.pipeTo(exchangeAccountDataManager)
  }

  def deliverOpenOrders(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    httpRequestJsonBinanceAccount[List[OpenOrderJson], JsValue](HttpMethods.GET, s"$BinanceBaseRestEndpoint/api/v3/openOrders", None, exchangeConfig.secrets, sign = true)
      .map {
        case Left(response) => IncomingData(exchangeDataMapping(response))
        case Right(errorResponse) => throw new RuntimeException(s"deliverOpenOrders failed: $errorResponse")
      }.pipeTo(exchangeAccountDataManager)
  }

  def pingUserStream(): Unit = {
    import DefaultJsonProtocol._
    httpRequestJsonBinanceAccount[String, String](HttpMethods.PUT,
      s"$BinanceBaseRestEndpoint/api/v3/userDataStream?listenKey=$listenKey", None, exchangeConfig.secrets, sign = false)
  }

  // fire and forget - error logging in case of failure
  def cancelOrder(tradePair: TradePair, externalOrderId: Long): Future[CancelOrderResult] = {
    val symbol = binanceTradePairsByTradePair(tradePair).symbol
    httpRequestPureJsonBinanceAccount(
      HttpMethods.DELETE,
      s"$BinanceBaseRestEndpoint/api/v3/order?symbol=$symbol&orderId=$externalOrderId",
      None,
      exchangeConfig.secrets,
      sign = true
    ).map {
      case (statusCode: StatusCode, response: JsValue) if statusCode.isSuccess() =>
        val r: JsObject = response.asJsObject
        if (r.fields.contains("status")
          && r.fields("status").convertTo[String] == "CANCELED"
          && r.fields.contains("orderId") && r.fields("orderId").convertTo[Long] == externalOrderId) {
          log.debug(s"Order successfully cancelled: $response")
          CancelOrderResult(exchangeConfig.exchangeName, tradePair, externalOrderId.toString, success = true, None)
        } else {
          log.warn(s"CancelOrder did not succeed: $r")
          CancelOrderResult(exchangeConfig.exchangeName, tradePair, externalOrderId.toString, success = false, Some(r.compactPrint))
        }
      case (statusCode: StatusCode, response: JsValue) => throw new RuntimeException(s"CancelOrder failed: $statusCode, $response")
    } recover {
      case e: Exception =>
        log.error(s"CancelOrder failed", e)
        CancelOrderResult(exchangeConfig.exchangeName, tradePair, externalOrderId.toString, success = false, Some(e.getMessage))
    }
  }


  def newLimitOrder(o: OrderRequest): Future[NewOrderAck] = {

    def alignToStepSize(amount: Double, stepSize: Double): Double = {
      stepSize * (amount / stepSize).ceil
    }

    def alignToTickSize(price: Double, tickSize: Double): Double = {
      if (tickSize == 0.0) price
      else tickSize * (price / tickSize).round
    }

    if (o.amountBaseAsset < 0.0) throw new WrongAssumption("our order amount is always positive")

    val binanceTradePair = binanceTradePairsByTradePair(o.tradePair)
    val price: Double = alignToTickSize(o.limit, binanceTradePair.tickSize)
    val quantity: Double = alignToStepSize(o.amountBaseAsset, binanceTradePair.lotSize.stepSize) match {
      case q: Double if price * q < binanceTradePair.minNotional =>
        val newQuantity = alignToStepSize(binanceTradePair.minNotional / price, binanceTradePair.lotSize.stepSize)
        log.info(s"binance: ${o.shortDesc} increasing quantity from ${o.amountBaseAsset} to ${formatDecimal(newQuantity)} to match min_notional filter")
        newQuantity
      case q: Double => q
    }
    // saftey check
    if (quantity > o.amountBaseAsset * 2)
      throw new BadCalculationError(s"Trade amount was much to small: ${o.shortDesc}. Required min-notional is " +
        s"${formatDecimal(binanceTradePair.minNotional)}, so for that limit we need a minimum quantity of ${formatDecimal(quantity)} (next time)!")
    if (price > o.limit * 1.05)
      throw new BadCalculationError(s"resulting price ${formatDecimal(price)} is > 105% of original limit ${formatDecimal(o.limit)}")

    val requestBody: String =
      s"symbol=${binanceTradePair.symbol}" +
        s"&side=${BinanceOrder.toString(o.tradeSide)}" +
        s"&type=${BinanceOrder.toString(OrderType.LIMIT)}" +
        s"&timeInForce=GTC" +
        s"&quantity=${formatDecimalWithFixPrecision(quantity, binanceTradePair.baseAssetPrecision)}" +
        s"&price=${formatDecimalWithFixPrecision(price, binanceTradePair.quotePrecision)}" +
        s"&newClientOrderId=${o.id.toString}" +
        s"&newOrderRespType=FULL"

    val response: Future[NewOrderResponseFullJson] = httpRequestJsonBinanceAccount[NewOrderResponseFullJson, JsValue](
      HttpMethods.POST,
      s"$BinanceBaseRestEndpoint/api/v3/order",
      Some(requestBody),
      exchangeConfig.secrets,
      sign = true
    ).map {
      case Left(response) => response
      case Right(errorResponse) => throw new RuntimeException(s"newLimitOrder failed: $errorResponse")
    }.recover {
      case e: Exception =>
        log.error(s"NewLimitOrder failed. Request body:\n$requestBody\nbinanceTradePair:$binanceTradePair\n", e)
        throw e
    }

    response.map(e => IncomingData(exchangeDataMapping(Seq(e)))).pipeTo(exchangeAccountDataManager)
    response.map(_.toNewOrderAck(exchangeConfig.exchangeName, symbol => binanceTradePairsBySymbol(symbol).toTradePair, o.id))
  }

  def createListenKey(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    listenKey = Await.result(
      httpRequestJsonBinanceAccount[ListenKeyJson, JsValue](HttpMethods.POST, s"$BinanceBaseRestEndpoint/api/v3/userDataStream", None, exchangeConfig.secrets, sign = false),
      globalConfig.httpTimeout.plus(500.millis)) match {
      case Left(response) => response.listenKey
      case Right(errorResponse) => throw new RuntimeException(s"createListenKey failed: $errorResponse")
    }
    log.trace(s"got listenKey: $listenKey")
  }

  def pullBinanceTradePairs(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    val binanceTradePairs: Set[BinanceTradePair] = Await.result(
      (exchangePublicDataInquirer ? GetBinanceTradePairs()).mapTo[Set[BinanceTradePair]],
      timeout.duration.plus(500.millis))

    binanceTradePairsBySymbol = binanceTradePairs.map(e => e.symbol -> e).toMap
    binanceTradePairsByTradePair = binanceTradePairs.map(e => e.toTradePair -> e).toMap
  }

  // is called, when all data streams are running
  def onStreamsRunning(): Unit = {
    deliverAccountInformation()
    deliverOpenOrders()
    exchangeAccountDataManager ! ExchangeAccountDataManager.Initialized()
  }

  override def postStop(): Unit = {
    // TODO DELETE /api/v3/userDataStream?listenKey=
    // https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md
  }

  def connect(): Unit = {
    log.trace("starting WebSocket stream")
    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    ws._2.future.onComplete(e => log.info(s"connection closed: ${e.get}"))
    connected = createConnected
  }

  override def preStart(): Unit = {
    try {
      createListenKey()
      pullBinanceTradePairs()
      self ! Connect()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }


  // @formatter:off
  override def receive: Receive = {
    case Connect()                               => connect()
    case SendPing()                              => pingUserStream()
    case CancelOrder(tradePair, externalOrderId) => cancelOrder(tradePair, externalOrderId.toLong).pipeTo(sender())
    case NewLimitOrder(o)                        => newLimitOrder(o).pipeTo(sender())
  }
  // @formatter:oon
}


case class ListenKeyJson(listenKey: String)
case class AccountStreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

trait IncomingBinanceAccountJson

case class BalanceJson(asset: String, free: String, locked: String) {
  def toBalance: Option[Balance] = {
    if (free.toDouble == 0.0 && locked.toDouble == 0.0)
      None
    else if (!StaticConfig.AllAssets.keySet.contains(asset))
      throw new Exception(s"We need to ignore a filled balance here, because it's asset is unknown: $this")
    else
      Some(adapter.Balance(
        Asset(asset),
        free.toDouble,
        locked.toDouble
      ))
  }
}

case class AccountInformationJson(makerCommission: Int,
                                  takerCommission: Int,
                                  buyerCommission: Int,
                                  sellerCommission: Int,
                                  canTrade: Boolean,
                                  canWithdraw: Boolean,
                                  canDeposit: Boolean,
                                  updateTime: Long,
                                  accountType: String,
                                  balances: Vector[BalanceJson],
                                  permissions: Vector[String]) extends IncomingBinanceAccountJson {
  def toWalletAssetUpdate: WalletAssetUpdate = WalletAssetUpdate(
    balances
      .flatMap(_.toBalance)
      .map(e => (e.asset, e))
      .toMap
  )
}

case class OutboundAccountInfoBalanceJson(a: String, f: String, l: String) {
  def toBalance: Option[Balance] = BalanceJson(a, f, l).toBalance
}

case class OutboundAccountInfoJson(e: String, // event type
                                   E: Long, // event time
                                   m: Int, // make commision rate (bips)
                                   t: Int, // taker commission rate (bips)
                                   b: Int, // buyer commission rate (bips)
                                   s: Int, // seller commission rate (bips)
                                   T: Boolean, // can trade?
                                   W: Boolean, // can withdraw?
                                   D: Boolean, // can deposit?
                                   u: Long, // time of last account update
                                   B: Vector[OutboundAccountInfoBalanceJson], // balances array
                                   P: Vector[String] // account permissions (e.g. SPOT)
                                  ) extends IncomingBinanceAccountJson

case class OutboundAccountPositionJson(e: String, // event type
                                       E: Long, // event time
                                       u: Long, // time of last account update
                                       B: Vector[OutboundAccountInfoBalanceJson] // balances
                                      ) extends IncomingBinanceAccountJson {
  def toWalletAssetUpdate: WalletAssetUpdate = WalletAssetUpdate(
    B.flatMap(_.toBalance)
      .map(e => (e.asset, e))
      .toMap)
}

case class BalanceUpdateJson(e: String, // event type
                             E: Long, // event time
                             a: String, // asset
                             d: String, // balance delta
                             T: Long // clear time
                            ) extends IncomingBinanceAccountJson {
  def toWalletBalanceUpdate: WalletBalanceUpdate = {
    if (!StaticConfig.AllAssets.keySet.contains(a)) throw new Exception(s"We need to ignore a BalanceUpdateJson here, because it's asset is unknown: $this")
    adapter.WalletBalanceUpdate(Asset(a), d.toDouble)
  }
}

case class OpenOrderJson(symbol: String,
                         orderId: Long,
                         // orderListId: Long,
                         clientOrderId: String,
                         price: String,
                         origQty: String,
                         executedQty: String,
                         cumulativeQuoteQty: String,
                         status: String,
                         //timeInForce: String,
                         `type`: String,
                         side: String,
                         stopPrice: String,
                         // icebergQty: String,
                         time: Long,
                         updateTime: Long,
                         isWorking: Boolean
                         // origQuoteOrderQty: String
                        ) extends IncomingBinanceAccountJson {
  def toOrderUpdate(exchange: String, resolveTradePair: String => TradePair): OrderUpdate = OrderUpdate(
    orderId.toString,
    exchange,
    resolveTradePair(symbol),
    toTradeSide(side),
    toOrderType(`type`),
    price.toDouble,
    stopPrice.toDouble match {
      case 0.0 => None
      case x => Some(x)
    },
    Some(origQty.toDouble),
    Some(Instant.ofEpochMilli(time)),
    Some(toOrderStatus(status)),
    cumulativeQuoteQty.toDouble,
    cumulativeQuoteQty.toDouble / executedQty.toDouble,
    Instant.ofEpochMilli(updateTime))
}


// see https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#enum-definitions
case class OrderExecutionReportJson(e: String, // Event type
                                    E: Long, // Event time
                                    s: String, // Symbol
                                    c: String, // Client order ID
                                    S: String, // Side (BUY, SELL)
                                    o: String, // Order type (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
                                    // f: String, // Time in force (GTC - Good Til Canceled, IOC - Immediate Or Cancel, FOK - Fill or Kill)
                                    q: String, // Order quantity (e.g. "1.00000000")
                                    p: String, // Order price
                                    P: String, // Stop price
                                    // F: String, // Iceberg quantity
                                    g: Long, // OrderListId
                                    C: String, // Original client order ID; This is the ID of the order being canceled (or null otherwise)
                                    x: String, // Current execution type (NEW - The order has been accepted into the engine, CANCELED - canceled by user, (REJECTED), TRADE - part of the order or all of the order's quantity has filled, EXPIRED - the order was canceled according to the order's rules)
                                    X: String, // Current order status (e.g. NEW, PARTIALLY_FILLED, FILLED, CANCELED, PENDING_CANCEL, REJECTED, EXPIRED)
                                    r: String, // Order reject reason; will be an error code or "NONE"
                                    i: Long, // Order ID
                                    // l: String, // Last executed quantity
                                    z: String, // Cumulative filled quantity (Average price can be found by doing Z divided by z)
                                    // L: String, // Last executed price
                                    // n: String, // Commission amount
                                    // N: String, // Commission asset (null)
                                    T: Long, // Transaction time
                                    t: Long, // Trade ID
                                    // I: Long, // Ignore
                                    w: Boolean, // Is the order on the book?
                                    m: Boolean, // Is this trade the maker side?
                                    // M: Boolean, // Ignore
                                    O: Long, // Order creation time
                                    Z: String // Cumulative quote asset transacted quantity
                                    // Y: String, // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
                                    // Q: String // Quote Order Qty
                                   ) extends IncomingBinanceAccountJson {

  // creationTime, orderPrice, stopPrice, originalQuantity
  def toOrderUpdate(exchange:String, resolveTradePair: String => TradePair): OrderUpdate = OrderUpdate(
    i.toString,
    exchange,
    resolveTradePair(s),
    toTradeSide(S),
    toOrderType(o),
    p.toDouble,
    P.toDouble match {
      case 0.0 => None
      case x => Some(x)
    },
    Some(q.toDouble),
    Some(Instant.ofEpochMilli(O)),
    Some(toOrderStatus(X)),
    z.toDouble,
    Z.toDouble / z.toDouble,
    Instant.ofEpochMilli(E))
}

object BinanceOrder {
  def toTradeSide(s: String): TradeSide = s match {
    case "BUY" => TradeSide.Buy
    case "SELL" => TradeSide.Sell
  }

  def toString(s: TradeSide): String = s match {
    case TradeSide.Buy => "BUY"
    case TradeSide.Sell => "SELL"
  }

  def toOrderType(o: String): OrderType = o match {
    // @formatter:off
      case "LIMIT"              => OrderType.LIMIT
      case "MARKET"             => OrderType.MARKET
      case "STOP_LOSS"          => OrderType.STOP_LOSS
      case "STOP_LOSS_LIMIT"    => OrderType.STOP_LOSS_LIMIT
      case "TAKE_PROFIT"        => OrderType.TAKE_PROFIT
      case "TAKE_PROFIT_LIMIT"  => OrderType.TAKE_PROFIT_LIMIT
      case "LIMIT_MAKER"        => OrderType.LIMIT_MAKER
      // @formatter:on
  }

  def toString(o: OrderType): String = o match {
    case OrderType.LIMIT => "LIMIT"
    case OrderType.MARKET => "MARKET"
    case OrderType.STOP_LOSS => "STOP_LOSS"
    case OrderType.STOP_LOSS_LIMIT => "STOP_LOSS_LIMIT"
    case OrderType.TAKE_PROFIT => "TAKE_PROFIT"
    case OrderType.TAKE_PROFIT_LIMIT => "TAKE_PROFIT_LIMIT"
    case OrderType.LIMIT_MAKER => "LIMIT_MAKER"
  }

  def toOrderStatus(X: String): OrderStatus = X match {
    // @formatter:off
      case "NEW"              => OrderStatus.NEW
      case "PARTIALLY_FILLED" => OrderStatus.PARTIALLY_FILLED
      case "FILLED"           => OrderStatus.FILLED
      case "CANCELED"         => OrderStatus.CANCELED
      case "REJECTED"         => OrderStatus.REJECTED
      case "EXPIRED"          => OrderStatus.EXPIRED
      // @formatter:on
  }
}


case class NewOrderResponseFillJson(price: String, qty: String, commission: String, commissionAsset: String)
case class NewOrderResponseFullJson(symbol: String,
                                    orderId: Long,
                                    orderListId: Long,
                                    clientOrderId: String,
                                    transactTime: Long,
                                    price: String,
                                    origQty: String,
                                    executedQty: String,
                                    cummulativeQuoteQty: String,
                                    status: String,
                                    timeInForce: String,
                                    `type`: String,
                                    side: String,
                                    fills: Vector[NewOrderResponseFillJson]
                                   ) extends IncomingBinanceAccountJson {

  def priceAverage(qtyPricePairs: Seq[Tuple2[Double, Double]]): Double =
    qtyPricePairs.map(e => e._1 * e._2).sum / qtyPricePairs.map(_._1).sum

  def toOrderUpdate(exchangeName: String, resolveTradePair: String => TradePair): OrderUpdate = {
    val ts = Instant.ofEpochMilli(transactTime)
    OrderUpdate(
      orderId.toString,
      exchangeName,
      resolveTradePair(symbol),
      BinanceOrder.toTradeSide(side),
      BinanceOrder.toOrderType(`type`),
      price.toDouble,
      None,
      Some(origQty.toDouble),
      Some(ts),
      Some(BinanceOrder.toOrderStatus(status)),
      fills.map(_.qty.toDouble).sum,
      priceAverage(fills.map(e => (e.qty.toDouble, e.price.toDouble))), // Seq(qty, price)
      ts)
  }

  def toNewOrderAck(exchange: String, symbolToTradePair: String => TradePair, ourOrderId: UUID): NewOrderAck =
    NewOrderAck(exchange, symbolToTradePair(symbol), orderId.toString, ourOrderId)
}


object BinanceAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val listenKeyJson: RootJsonFormat[ListenKeyJson] = jsonFormat1(ListenKeyJson)
  implicit val subscribeMsg: RootJsonFormat[AccountStreamSubscribeRequestJson] = jsonFormat3(AccountStreamSubscribeRequestJson)
  implicit val balanceJson: RootJsonFormat[BalanceJson] = jsonFormat3(BalanceJson)
  implicit val accountInformationJson: RootJsonFormat[AccountInformationJson] = jsonFormat11(AccountInformationJson)
  implicit val outboundAccountInfoBalanceJson: RootJsonFormat[OutboundAccountInfoBalanceJson] = jsonFormat3(OutboundAccountInfoBalanceJson)
  implicit val outboundAccountInfoJson: RootJsonFormat[OutboundAccountInfoJson] = jsonFormat12(OutboundAccountInfoJson)
  implicit val outboundAccountPositionJson: RootJsonFormat[OutboundAccountPositionJson] = jsonFormat4(OutboundAccountPositionJson)
  implicit val balanceUpdateJson: RootJsonFormat[BalanceUpdateJson] = jsonFormat5(BalanceUpdateJson)
  implicit val orderExecutionReportJson: RootJsonFormat[OrderExecutionReportJson] = jsonFormat22(OrderExecutionReportJson)
  implicit val openOrderJson: RootJsonFormat[OpenOrderJson] = jsonFormat14(OpenOrderJson)
  implicit val newOrderResponseFillJson: RootJsonFormat[NewOrderResponseFillJson] = jsonFormat4(NewOrderResponseFillJson)
  implicit val newOrderResponseResultJson: RootJsonFormat[NewOrderResponseFullJson] = jsonFormat14(NewOrderResponseFullJson)
}
