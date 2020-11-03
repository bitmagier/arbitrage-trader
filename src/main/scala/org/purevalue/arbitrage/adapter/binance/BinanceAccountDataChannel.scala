package org.purevalue.arbitrage.adapter.binance

import java.time.Instant
import java.util.UUID

import akka.Done
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinanceHttpUtil.httpRequestJsonBinanceAccount
import org.purevalue.arbitrage.adapter.binance.BinanceOrder.{toOrderStatus, toOrderType, toTradeSide}
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.{BinanceBaseRestEndpoint, GetBinanceTradePairs}
import org.purevalue.arbitrage.adapter.{AccountDataChannel, PublicDataInquirer}
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.{Balance, Exchange, ExchangeAccountStreamData, TickerSnapshot, Wallet, WalletAssetUpdate, WalletBalanceUpdate}
import org.purevalue.arbitrage.util.Util.{alignToStepSizeCeil, alignToStepSizeNearest, formatDecimal, formatDecimalWithFixPrecision}
import org.purevalue.arbitrage.util.{BadCalculationError, ConnectionClosedException, WrongAssumption}
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}


private[binance] case class ListenKeyJson(listenKey: String)
private[binance] case class AccountStreamSubscribeRequestJson(method: String = "SUBSCRIBE", params: Seq[String], id: Int)

private[binance] trait IncomingBinanceAccountJson

private[binance] case class BalanceJson(asset: String, free: String, locked: String) {
  def toBalance: Option[Balance] = {
    if (free.toDouble == 0.0 && locked.toDouble == 0.0)
      None
    else if (!Asset.isKnown(asset))
      throw new Exception(s"We need to ignore a filled balance here, because it's asset is unknown: $this")
    else
      Some(Balance(
        Asset(asset),
        free.toDouble,
        locked.toDouble
      ))
  }
}

private[binance] case class AccountInformationJson(makerCommission: Int,
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

private[binance] case class OutboundAccountBalanceJson(a: String, f: String, l: String) {
  def toBalance: Option[Balance] = BalanceJson(a, f, l).toBalance
}

// {"B":[{"a":"BTC","f":"0.00933255","l":"0.00000000"},{"a":"BNB","f":"10.49918278","l":"0.00000000"},{"a":"USDT","f":"782.25814525","l":"0.00000000"}],"E":1601137542661,"e":"outboundAccountPosition","u":1601137542660}
private[binance] case class OutboundAccountPositionJson(e: String, // event type
                                                        E: Long, // event time
                                                        u: Long, // time of last account update
                                                        B: Vector[OutboundAccountBalanceJson] // balances
                                                       ) extends IncomingBinanceAccountJson {
  def toWalletAssetUpdate: WalletAssetUpdate = WalletAssetUpdate(
    B.flatMap(_.toBalance)
      .map(e => (e.asset, e))
      .toMap)
}

private[binance] case class BalanceUpdateJson(e: String, // event type
                                              E: Long, // event time
                                              a: String, // asset
                                              d: String, // balance delta
                                              T: Long // clear time
                                             ) extends IncomingBinanceAccountJson {
  def toWalletBalanceUpdate: WalletBalanceUpdate = {
    if (!Asset.isKnown(a)) throw new Exception(s"We need to ignore a BalanceUpdateJson here, because it's asset is unknown: $this")
    WalletBalanceUpdate(Asset(a), d.toDouble)
  }
}

private[binance] case class OpenOrderJson(symbol: String,
                                          orderId: Long,
                                          // orderListId: Long,
                                          clientOrderId: String,
                                          price: String,
                                          origQty: String,
                                          executedQty: String,
                                          cummulativeQuoteQty: String, // yes cumulative has only one 'm', but this is what the API delivers :-)
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
    Some(toOrderType(`type`)),
    Some(price.toDouble),
    stopPrice.toDouble match {
      case 0.0 => None
      case x => Some(x)
    },
    Some(origQty.toDouble),
    Some(Instant.ofEpochMilli(time)),
    Some(toOrderStatus(status)),
    Some(cummulativeQuoteQty.toDouble),
    None,
    Some(cummulativeQuoteQty.toDouble / executedQty.toDouble),
    Instant.ofEpochMilli(updateTime))
}


// see https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#enum-definitions
private[binance] case class OrderExecutionReportJson(e: String, // Event type
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
  def toOrderUpdate(exchange: String, resolveTradePair: String => TradePair): OrderUpdate = OrderUpdate(
    i.toString,
    exchange,
    resolveTradePair(s),
    toTradeSide(S),
    Some(toOrderType(o)),
    Some(p.toDouble),
    P.toDouble match {
      case 0.0 => None
      case x => Some(x)
    },
    Some(q.toDouble),
    Some(Instant.ofEpochMilli(O)),
    Some(toOrderStatus(X)),
    Some(z.toDouble),
    None,
    Some(Z.toDouble / z.toDouble),
    Instant.ofEpochMilli(E))
}

private[binance] object BinanceOrder {
  def toTradeSide(s: String): TradeSide = s match {
    case "BUY" => TradeSide.Buy
    case "SELL" => TradeSide.Sell
  }

  def toString(s: TradeSide): String = s match {
    case TradeSide.Buy => "BUY"
    case TradeSide.Sell => "SELL"
  }

  // @formatter:off
  def toOrderType(o: String): OrderType = o match {
    case "LIMIT"              => OrderType.LIMIT
    case "MARKET"             => OrderType.MARKET
    case "STOP_LOSS"          => OrderType.STOP_LOSS
    case "STOP_LOSS_LIMIT"    => OrderType.STOP_LOSS_LIMIT
    case "TAKE_PROFIT"        => OrderType.TAKE_PROFIT
    case "TAKE_PROFIT_LIMIT"  => OrderType.TAKE_PROFIT_LIMIT
    case "LIMIT_MAKER"        => OrderType.LIMIT_MAKER
  } // @formatter:on

  // @formatter:off
  def toString(o: OrderType): String = o match {
    case OrderType.LIMIT             => "LIMIT"
    case OrderType.MARKET            => "MARKET"
    case OrderType.STOP_LOSS         => "STOP_LOSS"
    case OrderType.STOP_LOSS_LIMIT   => "STOP_LOSS_LIMIT"
    case OrderType.TAKE_PROFIT       => "TAKE_PROFIT"
    case OrderType.TAKE_PROFIT_LIMIT => "TAKE_PROFIT_LIMIT"
    case OrderType.LIMIT_MAKER       => "LIMIT_MAKER"
  } // @formatter:on

  // @formatter:off
  def toOrderStatus(X: String): OrderStatus = X match {
    case "NEW"              => OrderStatus.NEW
    case "PARTIALLY_FILLED" => OrderStatus.PARTIALLY_FILLED
    case "FILLED"           => OrderStatus.FILLED
    case "CANCELED"         => OrderStatus.CANCELED
    case "REJECTED"         => OrderStatus.REJECTED
    case "EXPIRED"          => OrderStatus.EXPIRED
  } // @formatter:on
}

private[binance] case class NewOrderResponseAckJson(symbol: String,
                                                    orderId: Long,
                                                    orderListId: Long,
                                                    clientOrderId: String,
                                                    transactTime: Long) {
  def toNewOrderAck(exchange: String, symbolToTradePair: String => TradePair): NewOrderAck =
    NewOrderAck(exchange, symbolToTradePair(symbol), orderId.toString, UUID.fromString(clientOrderId))
}

private[binance] case class CancelOrderResponseJson(symbol: String,
                                                    origClientOrderId: String,
                                                    orderId: Long,
                                                    orderListId: Long,
                                                    clientOrderId: String,
                                                    price: String,
                                                    origQty: String,
                                                    executedQty: String,
                                                    cummulativeQuoteQty: String,
                                                    status: String,
                                                    timeInForce: String,
                                                    `type`: String,
                                                    side: String) extends IncomingBinanceAccountJson {
  def toOrderUpdate(exchange: String, symbolToTradePair: String => TradePair): OrderUpdate = OrderUpdate(
    orderId.toString,
    exchange,
    symbolToTradePair(symbol),
    BinanceOrder.toTradeSide(side),
    Some(BinanceOrder.toOrderType(`type`)),
    Some(price.toDouble),
    None,
    Some(origQty.toDouble),
    None,
    Some(BinanceOrder.toOrderStatus(status)),
    Some(executedQty.toDouble),
    None,
    None,
    Instant.now // we don't have any better
  )
}

// @see https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
private[binance] case class ErrorResponseJson(code: Int, msg: String)

private[binance] object BinanceAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val listenKeyJson: RootJsonFormat[ListenKeyJson] = jsonFormat1(ListenKeyJson)
  implicit val subscribeMsg: RootJsonFormat[AccountStreamSubscribeRequestJson] = jsonFormat3(AccountStreamSubscribeRequestJson)
  implicit val balanceJson: RootJsonFormat[BalanceJson] = jsonFormat3(BalanceJson)
  implicit val accountInformationJson: RootJsonFormat[AccountInformationJson] = jsonFormat11(AccountInformationJson)
  implicit val outboundAccountInfoBalanceJson: RootJsonFormat[OutboundAccountBalanceJson] = jsonFormat3(OutboundAccountBalanceJson)
  implicit val outboundAccountPositionJson: RootJsonFormat[OutboundAccountPositionJson] = jsonFormat4(OutboundAccountPositionJson)
  implicit val balanceUpdateJson: RootJsonFormat[BalanceUpdateJson] = jsonFormat5(BalanceUpdateJson)
  implicit val orderExecutionReportJson: RootJsonFormat[OrderExecutionReportJson] = jsonFormat22(OrderExecutionReportJson)
  implicit val openOrderJson: RootJsonFormat[OpenOrderJson] = jsonFormat14(OpenOrderJson)
  implicit val newOrderResponseAckJson: RootJsonFormat[NewOrderResponseAckJson] = jsonFormat5(NewOrderResponseAckJson)
  implicit val cancelOrderResponseJson: RootJsonFormat[CancelOrderResponseJson] = jsonFormat13(CancelOrderResponseJson)
  implicit val errorResponseJson: RootJsonFormat[ErrorResponseJson] = jsonFormat2(ErrorResponseJson)
}


object BinanceAccountDataChannel {
  def apply(config: Config,
            exchangeConfig: ExchangeConfig,
            exchange: ActorRef[Exchange.Message],
            publicDataInquirer: ActorRef[PublicDataInquirer.Command]):
  Behavior[AccountDataChannel.Command] = {
    Behaviors.withTimers(timers =>
      Behaviors.setup(context => new BinanceAccountDataChannel(context, timers, config, exchangeConfig, exchange, publicDataInquirer)))
  }

  private case class Connect() extends AccountDataChannel.Command
  private case class SendPing() extends AccountDataChannel.Command
  private case class QueryAccountInformation() extends AccountDataChannel.Command
  private case class OnStreamsRunning() extends AccountDataChannel.Command
}

private[binance] class BinanceAccountDataChannel(context: ActorContext[AccountDataChannel.Command],
                                                 timers: TimerScheduler[AccountDataChannel.Command],
                                                 config: Config,
                                                 exchangeConfig: ExchangeConfig,
                                                 exchange: ActorRef[Exchange.Message],
                                                 publicDataInquirer: ActorRef[PublicDataInquirer.Command])
  extends AccountDataChannel(context) {

  import AccountDataChannel._
  import BinanceAccountDataChannel._

  // outboundAccountPosition is sent any time an account balance has changed and contains the assets
  // that were possibly changed by the event that generated the balance change
  val OutboundAccountPositionStreamName = "outboundAccountPosition"
  val IdOutboundAccountPositionStream: Int = 1
  // Balance Update occurs during the following:
  // - Deposits or withdrawals from the account
  // - Transfer of funds between accounts (e.g. Spot to Margin)
  val BalanceUpdateStreamName = "balanceUpdate"
  val IdBalanceUpdateStream: Int = 2
  // Orders are updated with the executionReport event
  val OrderExecutionReportStreamName = "executionReport"
  val IdOrderExecutionResportStream: Int = 3

  var outstandingStreamSubscribeResponses: Set[Int] = Set(IdOutboundAccountPositionStream, IdBalanceUpdateStream, IdOrderExecutionResportStream)

  @volatile var listenKey: String = _

  var binanceTradePairsBySymbol: Map[String, BinanceTradePair] = _
  var binanceTradePairsByTradePair: Map[TradePair, BinanceTradePair] = _

  lazy val WebSocketEndpoint: Uri = Uri(s"wss://stream.binance.com:9443/ws/$listenKey")
  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
  var connected: Future[Done.type] = _


  import BinanceAccountDataJsonProtocoll._


  def exchangeDataMapping(in: Seq[IncomingBinanceAccountJson]): Seq[ExchangeAccountStreamData] = in.map {
    // @formatter:off
    case a: AccountInformationJson      => a.toWalletAssetUpdate // deprecated
    case a: OutboundAccountPositionJson => a.toWalletAssetUpdate
    case b: BalanceUpdateJson           => b.toWalletBalanceUpdate
    case o: OrderExecutionReportJson    => o.toOrderUpdate(exchangeConfig.name, symbol => binanceTradePairsBySymbol(symbol).toTradePair)
    case o: OpenOrderJson               => o.toOrderUpdate(exchangeConfig.name, symbol => binanceTradePairsBySymbol(symbol).toTradePair)
    case o: CancelOrderResponseJson     => o.toOrderUpdate(exchangeConfig.name, symbol => binanceTradePairsBySymbol(symbol).toTradePair)
    case x                              => context.log.debug(s"$x"); throw new NotImplementedError
    // @formatter:on
  }

  def onStreamSubscribeResponse(j: JsObject): Unit = {
    if (context.log.isDebugEnabled) context.log.debug(s"received $j")
    val channelId = j.fields("id").convertTo[Int]

    val initialized: Boolean = synchronized {
      outstandingStreamSubscribeResponses = outstandingStreamSubscribeResponses - channelId
      outstandingStreamSubscribeResponses.isEmpty
    }

    if (initialized) {
      context.log.debug("all streams running")
      context.self ! OnStreamsRunning()
    }
  }

  def decodeMessage(message: Message): Future[Seq[IncomingBinanceAccountJson]] = message match {
    case msg: TextMessage =>
      try {
        msg.toStrict(config.global.httpTimeout)
          .map(_.getStrictText)
          .map(s => JsonParser(s).asJsObject())
          .map {
            case j: JsObject if j.fields.contains("result") =>
              onStreamSubscribeResponse(j)
              Nil
            case j: JsObject if j.fields.contains("e") =>
              j.fields("e").convertTo[String] match {
                case OutboundAccountPositionStreamName =>
                  if (context.log.isDebugEnabled) context.log.debug(s"received $j")
                  List(j.convertTo[OutboundAccountPositionJson])
                case BalanceUpdateStreamName =>
                  if (context.log.isDebugEnabled) context.log.debug(s"received $j")
                  List(j.convertTo[BalanceUpdateJson])
                case OrderExecutionReportStreamName =>
                  if (context.log.isDebugEnabled) context.log.debug(s"received $j")
                  List(j.convertTo[OrderExecutionReportJson])
                case "outboundAccountInfo" =>
                  if (context.log.isDebugEnabled) context.log.debug(s"watching obsolete outboundAccountInfo: $j")
                  Nil
                case name =>
                  context.log.error(s"Unknown data stream '$name' received: $j")
                  Nil
              }
            case j: JsObject =>
              context.log.warn(s"Unknown json object received: $j")
              Nil
          }
      } catch {
        case e: Throwable =>
          context.log.error("decodeMessage failed", e)
          Future.failed(e)
      }
    case x =>
      context.log.warn(s"Received non TextMessage: $x")
      Future.successful(Nil)
  }

  def deliverAccountInformation(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    httpRequestJsonBinanceAccount[AccountInformationJson, JsValue](
      HttpMethods.GET, s"$BinanceBaseRestEndpoint/api/v3/account", None, exchangeConfig.secrets, sign = true)
      .map {
        case Left(response) =>
          if (context.log.isDebugEnabled) context.log.debug(s"received initial account information: $response")
          IncomingAccountData(exchangeDataMapping(Seq(response)))
        case Right(errorResponse) => throw new RuntimeException(s"deliverAccountInformation failed: $errorResponse")
      }.foreach(data => exchange ! data)
  }

  def deliverOpenOrders(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    httpRequestJsonBinanceAccount[List[OpenOrderJson], JsValue](
      HttpMethods.GET, s"$BinanceBaseRestEndpoint/api/v3/openOrders", None, exchangeConfig.secrets, sign = true)
      .map {
        case Left(response) =>
          if (context.log.isDebugEnabled) context.log.debug(s"received initial open orders: $response")
          IncomingAccountData(exchangeDataMapping(response))
        case Right(errorResponse) => throw new RuntimeException(s"deliverOpenOrders failed: $errorResponse")
      }.foreach(data => exchange ! data)
  }

  def pingUserStream(): Unit = {
    import DefaultJsonProtocol._
    httpRequestJsonBinanceAccount[String, String](HttpMethods.PUT,
      s"$BinanceBaseRestEndpoint/api/v3/userDataStream?listenKey=$listenKey", None, exchangeConfig.secrets, sign = false)
  }

  def logWallet(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeout
    val wf = exchange.ask(ref => GetWallet(ref)).mapTo[Wallet]
    val tf = exchange.ask(ref => GetTickerSnapshot(ref)).mapTo[TickerSnapshot].map(_.ticker)
    (for {
      wallet <- wf
      ticker <- tf
    } yield (wallet, ticker)).foreach {
      case (wallet, ticker) =>
        context.log.info(s"[${exchangeConfig.name}] Wallet available crypto: ${wallet.availableCryptoValues()}\n" +
          s"liquid crypto: ${wallet.liquidCryptoValues(exchangeConfig.usdEquivalentCoin, ticker)}");
    }
  }

  def newLimitOrder(o: OrderRequest): Future[NewOrderAck] = {
    if (o.amountBaseAsset < 0.0) throw new WrongAssumption("our order amount is always positive")

    val binanceTradePair = binanceTradePairsByTradePair(o.pair)
    val price: Double = alignToStepSizeNearest(o.limit, binanceTradePair.tickSize)
    val quantity: Double = alignToStepSizeCeil(o.amountBaseAsset, binanceTradePair.lotSize.stepSize) match {
      case q: Double if price * q < binanceTradePair.minNotional =>
        val newQuantity = alignToStepSizeCeil(binanceTradePair.minNotional / price, binanceTradePair.lotSize.stepSize)
        context.log.info(s"binance: ${o.shortDesc} increasing quantity from ${o.amountBaseAsset} to ${formatDecimal(newQuantity)} to match min_notional filter")
        newQuantity
      case q: Double => q
    }
    // safety check
    if (quantity > o.amountBaseAsset * 2)
      throw new BadCalculationError(s"Trade amount was much to small: ${o.shortDesc}. Required min-notional is " +
        s"${formatDecimal(binanceTradePair.minNotional)}, so for that limit we need a minimum quantity of ${formatDecimal(quantity)} (next time)!")
    if (price > o.limit * 1.05)
      throw new BadCalculationError(s"resulting price ${formatDecimal(price)} is > 105% of original limit ${formatDecimal(o.limit)}")

    val requestBody: String =
      s"symbol=${binanceTradePair.symbol}" +
        s"&side=${BinanceOrder.toString(o.side)}" +
        s"&type=${BinanceOrder.toString(OrderType.LIMIT)}" +
        s"&timeInForce=GTC" +
        s"&quantity=${formatDecimalWithFixPrecision(quantity, binanceTradePair.baseAssetPrecision)}" +
        s"&price=${formatDecimalWithFixPrecision(price, binanceTradePair.quotePrecision)}" +
        s"&newClientOrderId=${o.id.toString}" +
        s"&newOrderRespType=ACK"

    val response: Future[NewOrderResponseAckJson] = httpRequestJsonBinanceAccount[NewOrderResponseAckJson, ErrorResponseJson](
      HttpMethods.POST,
      s"$BinanceBaseRestEndpoint/api/v3/order",
      Some(requestBody),
      exchangeConfig.secrets,
      sign = true
    ).map {
      case Left(response) => response
      case Right(errorResponse) =>
        errorResponse match {
          case ErrorResponseJson(BinanceErrorCodes.NewOrderRejected, "Account has insufficient balance for requested action.") =>
            logWallet()
          case _ =>
        }
        throw new RuntimeException(s"newLimitOrder(${o.shortDesc}) failed: $errorResponse")
    } recover {
      case e: Exception =>
        context.log.error(s"NewLimitOrder(${o.shortDesc}) failed. Request body:\n$requestBody\nbinanceTradePair:$binanceTradePair\n", e)
        throw e
    }

    response.map(_.toNewOrderAck(exchangeConfig.name, symbol => binanceTradePairsBySymbol(symbol).toTradePair))
  }

  // fire and forget - error logging in case of failure
  override def cancelOrder(ref: OrderRef): Future[Exchange.CancelOrderResult] = {
    val symbol = binanceTradePairsByTradePair(ref.pair).symbol
    httpRequestJsonBinanceAccount[CancelOrderResponseJson, ErrorResponseJson](
      HttpMethods.DELETE,
      s"$BinanceBaseRestEndpoint/api/v3/order?symbol=$symbol&orderId=${ref.externalOrderId}",
      None,
      exchangeConfig.secrets,
      sign = true
    ).map {
      case Left(response) =>
        if (context.log.isDebugEnabled) context.log.debug(s"Order successfully canceled: $response")
        exchange ! IncomingAccountData(exchangeDataMapping(Seq(response)))
        CancelOrderResult(exchangeConfig.name, ref.pair, ref.externalOrderId, success = true)
      case Right(errorResponse) =>
        context.log.debug(s"CancelOrder failed: $errorResponse")
        val orderUnknown: Boolean = errorResponse.code match {
          case BinanceErrorCodes.NoSuchOrder => true
          case BinanceErrorCodes.CancelRejected if errorResponse.msg == "Unknown order sent." => true
          case _ => false
        }
        CancelOrderResult(exchangeConfig.name, ref.pair, ref.externalOrderId, success = false, orderUnknown, Some(errorResponse.toString))
    }
  }

  def createListenKey(): Unit = {
    import BinanceAccountDataJsonProtocoll._
    listenKey = Await.result(
      httpRequestJsonBinanceAccount[ListenKeyJson, JsValue](HttpMethods.POST,
        s"$BinanceBaseRestEndpoint/api/v3/userDataStream", None, exchangeConfig.secrets, sign = false),

      config.global.httpTimeout.plus(500.millis)) match {
      case Left(response) => response.listenKey
      case Right(errorResponse) => throw new RuntimeException(s"createListenKey failed: $errorResponse")
    }
    context.log.debug(s"got listenKey: $listenKey")
  }

  def pullBinanceTradePairs(): Unit = {
    implicit val timeout: Timeout = config.global.internalCommunicationTimeoutDuringInit
    val binanceTradePairs: Set[BinanceTradePair] = Await.result(
      publicDataInquirer.ask(ref => GetBinanceTradePairs(ref)),
      timeout.duration.plus(500.millis))

    binanceTradePairsBySymbol = binanceTradePairs.map(e => e.symbol -> e).toMap
    binanceTradePairsByTradePair = binanceTradePairs.map(e => e.toTradePair -> e).toMap
  }

  // is called, when all data streams are running
  def onStreamsRunning(): Unit = {
    deliverAccountInformation()
    deliverOpenOrders()
    exchange ! Exchange.AccountDataChannelInitialized()
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
          .map(IncomingAccountData)
          .foreach(exchange ! _)
      ),
      Source(
        SubscribeMessages.map(msg => TextMessage(msg.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }


  def createConnected: Future[Done.type] =
    ws._1.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        context.log.info("connected")
        timers.startTimerAtFixedRate(SendPing(), 30.minutes)
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

  def connect(): Unit = {
    context.log.info(s"connecting WebSocket $WebSocketEndpoint ...")
    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    ws._2.future.onComplete { e =>
      context.log.info(s"connection closed")
      context.self ! ConnectionClosed("BinanceAccountDataChannel")
    }
    connected = createConnected
  }

  def init(): Unit = {
    context.log.info("initializing binance account data channel")
    try {
      createListenKey()
      pullBinanceTradePairs()
      context.self ! Connect()
    } catch {
      case e: Exception => context.log.error("init failed", e)
    }
  }

  // @formatter:off
  override def onMessage(message: Command): Behavior[Command] = message match {
    case Connect()                                    => connect(); this
    case OnStreamsRunning()                           => onStreamsRunning(); this
    case SendPing()                                   => pingUserStream(); this
    case AccountDataChannel.NewLimitOrder(o, replyTo) => newLimitOrder(o).foreach(ack => replyTo ! ack); this
    case c: AccountDataChannel.CancelOrder            => handleCancelOrder(c); this
    case ConnectionClosed(c)                          => throw new ConnectionClosedException(c) // in order to let the actor terminate and restart by
  }
  // @formatter:oon

  def postStop(): Unit = {
    // https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md
    if (ws != null && !ws._2.isCompleted) ws._2.success(None)
  }

  override def onSignal: PartialFunction[Signal, Behavior[AccountDataChannel.Command]] = {
    case PostStop =>
      postStop()
      context.log.info(s"${this.getClass.getSimpleName} stopped")
      this
  }

  init()
}

// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark