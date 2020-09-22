package org.purevalue.arbitrage.adapter.bitfinex

import java.time.Instant

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.ExchangeAccountDataManager._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexAccountDataChannel.Connect
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexOrderUpdateJson.{toOrderStatus, toOrderType}
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.{GetBitfinexAssets, GetBitfinexTradePairs}
import org.purevalue.arbitrage.adapter.{Balance, ExchangeAccountStreamData, WalletAssetUpdate}
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.util.{HttpUtil, WrongAssumption}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.collection.Seq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


trait IncomingBitfinexAccountJson

case class BitfinexAuthMessage(apiKey: String, authSig: String, authNonce: String, authPayload: String, filter: Array[String], event: String = "auth")
case class BitfinexOrderUpdateJson(streamType: String, // "os" = order snapshot, "on" = order new, "ou" = order update, "oc" = order cancel
                                   orderId: Long,
                                   groupId: Option[Long],
                                   clientOrderId: Long,
                                   symbol: String,
                                   createTime: Long,
                                   updateTime: Long,
                                   amount: Double, // positive means buy, negative means sell
                                   amountOriginal: Double,
                                   orderType: String, // The type of the order: LIMIT, MARKET, STOP, STOP LIMIT, TRAILING STOP, EXCHANGE MARKET, EXCHANGE LIMIT, EXCHANGE STOP, EXCHANGE STOP LIMIT, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC.
                                   orderTypePrev: Option[String],
                                   timeInForceTime: Option[Long],
                                   flags: Long,
                                   orderStatus: String, // Order Status: ACTIVE, EXECUTED @ PRICE(AMOUNT) e.g. "EXECUTED @ 107.6(-0.2)", PARTIALLY FILLED @ PRICE(AMOUNT), INSUFFICIENT MARGIN was: PARTIALLY FILLED @ PRICE(AMOUNT), CANCELED, CANCELED was: PARTIALLY FILLED @ PRICE(AMOUNT), RSN_DUST (amount is less than 0.00000001), RSN_PAUSE (trading is paused / paused due to AMPL rebase event)
                                   price: Double,
                                   priceAverage: Double,
                                   priceTrailing: Double,
                                   priceAuxLimit: Double, // Auxiliary Limit price (STOP_LIMIT)
                                   notifY: Int, // 0 if false, 1 if true
                                   hidden: Option[Int], // 1 if Hidden, 0 if not hidden
                                   placedId: Option[Long], // if another order caused this order to be placed (OCO) this will be that other order's ID
                                   routing: String // indicates origin of action: BFX, ETHFX, API>BFX, API>ETHFX
                                  ) extends IncomingBitfinexAccountJson {

  def extractBetweenRoundBrackets(s: String): String = {
    s.substring(s.indexOf('(') + 1, s.indexOf(')'))
  }

  def parseCumulativeFilled: Double = orderStatus match {
    case status: String if status.startsWith("EXECUTED") => extractBetweenRoundBrackets(status).toDouble // EXECUTED @ PRICE(AMOUNT) e.g. "EXECUTED @ 107.6(-0.2)"
    case status: String if status.startsWith("PARTIALLY FILLED") => extractBetweenRoundBrackets(status).toDouble // PARTIALLY FILLED @ PRICE(AMOUNT)
    case status: String if status.startsWith("CANCELLED was: PARTIALLY FILLED") => extractBetweenRoundBrackets(status).toDouble // CANCELED was: PARTIALLY FILLED @ PRICE(AMOUNT)
    case _ => 0.0
  }

  def toOrder(exchange: String, resolveTradePair: String => TradePair): Order = Order(
    orderId.toString,
    exchange,
    resolveTradePair(symbol),
    if (amount >= 0.0) TradeSide.Buy else TradeSide.Sell,
    toOrderType(orderType),
    price,
    if (priceAuxLimit != 0.0) Some(priceAuxLimit) else None, // TODO check what we get
    Math.abs(amount),
    orderStatus match {
      case s: String if s.startsWith("CANCELED") => Some(s)
      case s: String if s.startsWith("RSN_DUST") => Some(s)
      case s: String if s.startsWith("RSN_PAUSE") => Some(s)
      case s: String if s.startsWith("INSUFFICIENT MARGIN") => Some(s)
      case _ => None
    },
    Instant.ofEpochMilli(createTime),
    toOrderStatus(orderStatus),
    parseCumulativeFilled,
    priceAverage,
    Instant.ofEpochMilli(updateTime)
  )

  def toOrderUpdate(exchange: String, resolveTradePair: String => TradePair): OrderUpdate = OrderUpdate(
    orderId.toString,
    exchange,
    resolveTradePair(symbol),
    if (amount >= 0.0) TradeSide.Buy else TradeSide.Sell,
    toOrderType(orderType),
    price,
    if (priceAuxLimit != 0.0) Some(priceAuxLimit) else None, // TODO check what we get
    Some(Math.abs(amountOriginal)),
    Some(Instant.ofEpochMilli(createTime)),
    toOrderStatus(orderStatus),
    parseCumulativeFilled,
    priceAverage,
    Instant.ofEpochMilli(updateTime))

  def toOrderOrOrderUpdate(exchange: String, resolveTradePair: String => TradePair): ExchangeAccountStreamData = {
    if (streamType == "on") toOrder(exchange, resolveTradePair)
    else toOrderUpdate(exchange, resolveTradePair)
  }
}
object BitfinexOrderUpdateJson {

  import DefaultJsonProtocol._

  //  received event 'order update': [0,"os",[
  //  [51506597828,null,1600515007213,"tBTCUST",1600515007213,1600515007219,0.0022697,0.0022697,"EXCHANGE LIMIT",null,
  //  null, null,0,"ACTIVE",null,null,11015,0,0,0,
  //  null,null,null,0,0,null,null,null,"API>BFX",null,
  //  null,{"aff_code":"IUXlFHleA"}]]]
  def apply(streamType: String, v: Array[JsValue]): BitfinexOrderUpdateJson = BitfinexOrderUpdateJson(
    streamType,
    v(0).convertTo[Long],
    v(1).convertTo[Option[Long]],
    v(2).convertTo[Long],
    v(3).convertTo[String],
    v(4).convertTo[Long],
    v(5).convertTo[Long],
    v(6).convertTo[Double],
    v(7).convertTo[Double],
    v(8).convertTo[String],
    v(9).convertTo[Option[String]],
    v(10).convertTo[Option[Long]],
    v(12).convertTo[Long],
    v(13).convertTo[String],
    v(16).convertTo[Double],
    v(17).convertTo[Double],
    v(18).convertTo[Double],
    v(19).convertTo[Double],
    v(23).convertTo[Int],
    v(24).convertTo[Option[Int]],
    v(25).convertTo[Option[Long]],
    v(28).convertTo[String]
  )

  def toOrderType(orderType: String): OrderType = orderType match {
    case "LIMIT" => OrderType.LIMIT // used for margin/funding
    case "MARKET" => OrderType.MARKET // used for margin/funding
    case "STOP" => OrderType.STOP_LOSS // used for margin/funding
    case "STOP LIMIT" => OrderType.STOP_LOSS_LIMIT // used for margin/funding
    case "EXCHANGE MARKET" => OrderType.MARKET // this is what we get delivered from the bitfinex exchange market
    case "EXCHANGE LIMIT" => OrderType.LIMIT // this is what we get delivered from the bitfinex exchange market
    case "EXCHANGE STOP" => OrderType.STOP_LOSS // this is what we get delivered from the bitfinex exchange market
    case "EXCHANGE STOP LIMIT" => OrderType.STOP_LOSS_LIMIT // this is what we get delivered from the bitfinex exchange market
    // not implemented, because not used: TRAILING STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC.
    case unhandled: String => throw new RuntimeException(s"bitfinex unhandled ORDER_TYPE $unhandled")
  }


  def toOrderStatus(orderStatus: String): OrderStatus = orderStatus match {
    case "ACTIVE" => OrderStatus.NEW
    case s: String if s.startsWith("CANCELED") => OrderStatus.CANCELED
    case s: String if s.startsWith("EXECUTED") => OrderStatus.FILLED
    case s: String if s.startsWith("PARTIALLY FILLED") => OrderStatus.PARTIALLY_FILLED
    case s: String if s.startsWith("RSN_DUST") => OrderStatus.REJECTED
    case s: String if s.startsWith("INSUFFICIENT MARGIN") => OrderStatus.REJECTED
  }
}


case class BitfinexWalletUpdateJson(walletType: String,
                                    currency: String,
                                    balance: Double,
                                    unsettledInterest: Double,
                                    balanceAvailable: Option[Double], // maybe null if not fresh enough
                                    description: Option[String]
                                    // ignoring meta: json // Provides info on the reason for the wallet update, if available.
                                   ) extends IncomingBitfinexAccountJson {
  def toWalletAssetUpdate(resolveAsset: String => Asset): WalletAssetUpdate = WalletAssetUpdate(
    if (walletType == "exchange") {
      val asset = resolveAsset(currency)
      val balanceAvailable: Double = this.balanceAvailable.getOrElse(balance)
      Map(asset -> Balance(asset, balanceAvailable, balance - balanceAvailable))
    }
    else Map() // ignore "margin/funding" wallets
  )
}
object BitfinexWalletUpdateJson {

  import DefaultJsonProtocol._

  def apply(v: Array[JsValue]): BitfinexWalletUpdateJson =
    BitfinexWalletUpdateJson(
      v(0).convertTo[String],
      v(1).convertTo[String],
      v(2).convertTo[Double],
      v(3).convertTo[Double],
      v(4).convertTo[Option[Double]],
      v(5).convertTo[Option[String]])
}

case class SubmitLimitOrderJson(`type`: String,
                                symbol: String,
                                price: String,
                                amount: String, // Amount of order (positive for buy, negative for sell)
                                meta: JsObject)

case class OrderResponseJson(mts: Long,
                             orders: Array[BitfinexOrderUpdateJson],
                             status: String, // SUCCESS, ERROR. FAILURE, ...
                             text: String)

object BitfinexAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val bitfinexAuthMessage: RootJsonFormat[BitfinexAuthMessage] = jsonFormat6(BitfinexAuthMessage)
  implicit val submitLimitOrderJson: RootJsonFormat[SubmitLimitOrderJson] = jsonFormat5(SubmitLimitOrderJson)

  // [1600515007,
  // "on-req",
  // null,
  // null,
  // [[51506597828,null,1600515007213,"tBTCUST",1600515007213,1600515007213,0.0022697,0.0022697,"EXCHANGE LIMIT",null,null,null,0,"ACTIVE",null,null,11015,0,0,0,null,null,null,0,0,null,null,null,"API>BFX",null,null,{"aff_code":"IUXlFHleA"}]],
  // null,
  // "SUCCESS",
  // "Submitting 1 orders."]
  implicit object submitOrderResponseJson extends RootJsonFormat[OrderResponseJson] {
    def read(value: JsValue): OrderResponseJson = {
      val v = value.convertTo[Array[JsValue]]
      val orderUpdates: Array[BitfinexOrderUpdateJson] =
        v(4).convertTo[Array[Array[JsValue]]]
        .map(o => BitfinexOrderUpdateJson.apply("on", o))
      OrderResponseJson(v(0).convertTo[Long], orderUpdates, v(6).convertTo[String], v(7).convertTo[String])
    }

    def write(v: OrderResponseJson): JsValue = throw new NotImplementedError
  }
}

////////////////////////////////////////////////


object BitfinexAccountDataChannel {
  private case class Connect()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            exchangeAccountDataManager: ActorRef,
            publicDataInquirer: ActorRef): Props =
    Props(new BitfinexAccountDataChannel(globalConfig, exchangeConfig, exchangeAccountDataManager, publicDataInquirer))
}
class BitfinexAccountDataChannel(globalConfig: GlobalConfig,
                                 exchangeConfig: ExchangeConfig,
                                 exchangeAccountDataManager: ActorRef,
                                 exchangePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexAccountDataChannel])

  val BaseRestEndpoint = "https://api.bitfinex.com"
  val WebSocketEndpoint: Uri = Uri("wss://api.bitfinex.com/ws/2")

  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var bitfinexAssets: Set[BitfinexSymbol] = _
  var bitfinexTradePairs: Set[BitfinexTradePair] = _
  var bitfinexTradePairByApiSymbol: Map[String, BitfinexTradePair] = _
  var bitfinexTradePairByTradePair: Map[TradePair, BitfinexTradePair] = _

  import BitfinexAccountDataJsonProtocoll._

  def symbolToAsset(symbol: String): Asset = bitfinexAssets.find(_.apiSymbol == symbol) match {
    case Some(bitfinexAsset) => bitfinexAsset.asset
    case None => StaticConfig.AllAssets.find(_._1 == symbol) match {
      case Some(globalAsset) => globalAsset._2
      case None => throw new IllegalArgumentException(s"unable to find Asset for bitfinex symbol $symbol. Bitfinex has: $bitfinexAssets")
    }
  }

  def exchangeDataMapping(in: Seq[IncomingBitfinexAccountJson]): Seq[ExchangeAccountStreamData] = in.map {
    // @formatter:off
    case o: BitfinexOrderUpdateJson   => o.toOrderOrOrderUpdate(exchangeConfig.exchangeName, symbol => bitfinexTradePairByApiSymbol(symbol).toTradePair)
//    case t: BitfinexTradeExecutedJson => t.toOrderUpdate(exchangeConfig.exchangeName, symbol => bitfinexTradePairByApiSymbol(symbol).toTradePair)
    case w: BitfinexWalletUpdateJson  => w.toWalletAssetUpdate(symbol => symbolToAsset(symbol))
    case x                            => log.error(s"$x"); throw new NotImplementedError
    // @formatter:on
  }

  def decodeJsonObject(s: String): Unit = {
    JsonParser(s).asJsObject match {
      case j: JsObject if j.fields.contains("event") =>
        j.fields("event").convertTo[String] match {
          case "auth" if j.fields("status").convertTo[String] == "OK" =>
            log.trace(s"received auth response: $j")
            onStreamsRunning()
            None
          case "auth" =>
            throw new RuntimeException(s"bitfinex account WebSocket authentification failed with: $j")
          case _ =>
            log.info(s"bitfinex: watching event message: $s") // TODO log level
            None
        }
      case j: JsObject =>
        log.warn(s"Unknown json event object received: $j")
        None
    }
  }


  def decodeDataArray(s: String): Seq[IncomingBitfinexAccountJson] = {
    JsonParser(s).convertTo[List[JsValue]] match {
      case dataArray: List[JsValue] if dataArray.length >= 2 && dataArray.head.convertTo[Long] == 0 =>
        dataArray(1).convertTo[String] match {

          case "hb" =>
            if (log.isTraceEnabled) log.trace(s"received heartbeat event")
            Seq()

          case "os" => // order snapshot
            if (log.isTraceEnabled) log.trace(s"received event 'order snaphot': $s")
            dataArray(2).convertTo[Array[JsValue]]
              .map(e => BitfinexOrderUpdateJson("os", e.convertTo[Array[JsValue]]))

          case streamType: String if Seq("on", "ou", "oc").contains(streamType) =>
            if (log.isTraceEnabled) log.trace(s"received event 'order new/update/cancel': $s")
            Seq(BitfinexOrderUpdateJson(streamType, dataArray(2).convertTo[Array[JsValue]]))

          case streamType: String if Seq("ps", "pn", "pu", "pc").contains(streamType) =>
            log.debug(s"ignoring 'Position' data: $s")
            Seq()

          case "tu" =>
            if (log.isTraceEnabled) log.trace(s"ignoring trade update event: $s")
            Seq() // ignoring trade updates (prefering order updates, which have more details)

          case "te" =>
            if (log.isTraceEnabled) log.trace(s"received event 'trade executed': $s")
            // we can ignore trade events because we have order events
            //            Seq(BitfinexTradeExecutedJson(dataArray(2).convertTo[Array[JsValue]]))
            Seq()

          case "ws" =>
            if (log.isTraceEnabled) log.trace(s"received event 'wallet snapshot': $s")
            dataArray(2).convertTo[Array[JsValue]].map(e => BitfinexWalletUpdateJson(e.convertTo[Array[JsValue]]))

          case "wu" =>
            if (log.isTraceEnabled) log.trace(s"received event 'wallet update': $s")
            Seq(BitfinexWalletUpdateJson(dataArray(2).convertTo[Array[JsValue]]))

          case "bu" =>
            if (log.isTraceEnabled) log.trace(s"watching event 'balance update': $s")
            Seq() // we ignore that event, we can calculate balance from wallet

          case "miu" =>
            if (log.isTraceEnabled) log.trace(s"watching event 'margin info update': $s")
            Seq() // ignore margin info update

          case "fiu" =>
            if (log.isTraceEnabled) log.trace(s"watching event 'funding info': $s")
            Seq() // ignore funding info

          case s: String if Seq("fte", "ftu").contains(s) =>
            if (log.isTraceEnabled) log.trace(s"watching event 'funding trade': $s")
            Seq() // ignore funding trades

          case "n" =>
            if (log.isTraceEnabled) log.trace(s"received notification event: $s")
            Seq() // ignore notifications

          case x => throw new RuntimeException(s"bitfinex: received data of unidentified stream type '$x': $s")
        }
    }
  }

  def decodeMessage(message: Message): Future[Seq[IncomingBitfinexAccountJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map {
          case s: String if s.startsWith("{") => decodeJsonObject(s); Nil
          case s: String if s.startsWith("[") => decodeDataArray(s)
          case x => throw new RuntimeException(s"unidentified response: $x")
        }
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(Nil)
  }

  def authMessage: BitfinexAuthMessage = {
    val authNonce: String = Instant.now.toEpochMilli.toString
    val authPayload = "AUTH" + authNonce
    BitfinexAuthMessage(
      apiKey = exchangeConfig.secrets.apiKey,
      HttpUtil.hmacSha384Signature(authPayload, exchangeConfig.secrets.apiSecretKey),
      authNonce,
      authPayload,
      filter = Array("trading", "trading-tBTCUSD", "wallet", "wallet-exchange-BTC", "balance", "notify")
    )
  }

  val wsFlow: Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .map(exchangeDataMapping)
          .map(IncomingData)
          .pipeTo(exchangeAccountDataManager)
      ),
      Source(List(
        TextMessage(authMessage.toJson.compactPrint)
      )).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

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

  def pullBitfinexTradePairs(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    bitfinexTradePairs = Await.result(
      (exchangePublicDataInquirer ? GetBitfinexTradePairs()).mapTo[Set[BitfinexTradePair]],
      timeout.duration.plus(500.millis)
    )
    bitfinexTradePairByApiSymbol = bitfinexTradePairs.map(e => e.apiSymbol -> e).toMap
    bitfinexTradePairByTradePair = bitfinexTradePairs.map(e => e.toTradePair -> e).toMap
  }

  def pullBitfinexAssets(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    bitfinexAssets = Await.result(
      (exchangePublicDataInquirer ? GetBitfinexAssets()).mapTo[Set[BitfinexSymbol]],
      timeout.duration.plus(500.millis)
    )
  }

  def connect(): Unit = {
    if (log.isTraceEnabled) log.trace("starting WebSocket stream")
    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), wsFlow)
    ws._2.future.onComplete(e => log.info(s"connection closed: ${e.get}"))
    connected = createConnected
  }

  val MinPricePrecision: Int = 5
  val MaxPricePrecision: Int = 8

  def toSubmitLimitOrderJson(o: OrderRequest, resolveSymbol: TradePair => String, affiliateCode: Option[String]): SubmitLimitOrderJson = {
    SubmitLimitOrderJson(
      "EXCHANGE LIMIT", // has to be "EXCHANGE ..." see https://github.com/bitfinexcom/bitfinex-api-node/issues/220
      resolveSymbol.apply(o.tradePair),
      formatDecimal(o.limit, Math.min(Math.max(MinPricePrecision, o.tradePair.quoteAsset.defaultPrecision), MaxPricePrecision)),
      o.tradeSide match {
        case TradeSide.Buy => formatDecimal(o.amountBaseAsset, o.tradePair.baseAsset.defaultPrecision)
        case TradeSide.Sell => formatDecimal(-o.amountBaseAsset, o.tradePair.baseAsset.defaultPrecision)
      },
      affiliateCode match {
        case Some(code) => JsObject(Map("aff_code" -> JsString(code)))
        case None => JsObject()
      }
    )
  }

  def newLimitOrder(o: OrderRequest): Future[NewOrderAck] = {
    import BitfinexAccountDataJsonProtocoll._

    val requestBody = toSubmitLimitOrderJson(o, tp => bitfinexTradePairByTradePair(tp).apiSymbol, exchangeConfig.refCode).toJson.compactPrint
    HttpUtil.httpRequestJsonBitfinexAccount[OrderResponseJson](
      HttpMethods.POST,
      s"$BaseRestEndpoint/v2/auth/w/order/submit",
      Some(requestBody),
      exchangeConfig.secrets
    ).map {
      case r: OrderResponseJson if r.status == "SUCCESS" && r.orders.length == 1 =>
        if (r.orders.length > 1) throw new WrongAssumption("returned number of orders > 1")
        val order = r.orders.head
        exchangeAccountDataManager ! IncomingData(exchangeDataMapping(Seq(order)))
        NewOrderAck(exchangeConfig.exchangeName, o.tradePair, order.orderId.toString, o.id)
      case r: OrderResponseJson =>
        throw new RuntimeException(s"Something went wrong while placing a limit-order. Response is: $r")
    } recover {
      case e: Exception =>
        log.error(s"NewLimitOrder failed. Request body:\n$requestBody", e)
        throw e;
    }
  }

  def cancelOrder(tradePair: TradePair, externalOrderId: Long): Future[CancelOrderResult] = {
    import BitfinexAccountDataJsonProtocoll._

    HttpUtil.httpRequestJsonBitfinexAccount[OrderResponseJson](
      HttpMethods.POST,
      s"$BaseRestEndpoint/v2/auth/w/order/cancel",
      Some(s"{id:$externalOrderId}"),
      exchangeConfig.secrets
    ).map {
      case r: OrderResponseJson if r.status == "SUCCESS" && r.orders.length == 1 =>
        if (r.orders.length > 1) throw new WrongAssumption("returned number of orders > 1")
        val order = r.orders.head
        exchangeAccountDataManager ! IncomingData(exchangeDataMapping(Seq(order)))
        CancelOrderResult(exchangeConfig.exchangeName, tradePair, r.orders.head.orderId.toString, success = true)
      case r: OrderResponseJson =>
        log.debug(s"Cancel order failed. Result: $r")
        CancelOrderResult(exchangeConfig.exchangeName, tradePair, externalOrderId.toString, success = false)
    }
  }

  def onStreamsRunning(): Unit = {
    exchangeAccountDataManager ! Initialized()
  }

  override def preStart(): Unit = {
    try {
      pullBitfinexTradePairs()
      pullBitfinexAssets()
      self ! Connect()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case Connect()                               => connect()
    case CancelOrder(tradePair, externalOrderId) => cancelOrder(tradePair, externalOrderId.toLong).pipeTo(sender())
    case NewLimitOrder(o)                        => newLimitOrder(o).pipeTo(sender())
  }
  // @formatter:on
}


//case class BitfinexTradeExecutedJson(tradeId: Long,
//                                     symbol: String,
//                                     executionTime: Long,
//                                     orderId: Long,
//                                     execAmount: Double, // positive means buy, negative means sell
//                                     execPrice: Double,
//                                     orderType: String,
//                                     orderPrice: Double,
//                                     maker: Int,
//                                     clientOrderId: Long
//                                    ) // 1 if true, -1 if false
//// fee: Option[Double], // "tu" only
//// feeCurrency: Option[String] // "tu" only
//  extends IncomingBitfinexAccountJson {
//  def toOrderUpdate(exchange: String, resolveTradePair: String => TradePair): OrderUpdate = OrderUpdate(
//    orderId.toString,
//    exchange,
//    resolveTradePair(symbol),
//    if (execAmount >= 0.0) TradeSide.Buy else TradeSide.Sell,
//    toOrderType(orderType),
//    orderPrice,
//    None,
//    None,
//    None,
//    OrderStatus.FILLED,
//    execAmount,
//    orderPrice,
//    Instant.ofEpochMilli(executionTime)
//  )
//}
//object BitfinexTradeExecutedJson {
//
//  import DefaultJsonProtocol._
//
//  // received event 'trade executed':
//  // [0,"te",
//  // [505610475,"tBTCUST",1600519908739,51511737822,0.00180267,11092,"EXCHANGE LIMIT",11095,-1,null,
//  // null,1600519908736]]
//  def apply(v: Array[JsValue]): BitfinexTradeExecutedJson =
//    BitfinexTradeExecutedJson(
//      v(0).convertTo[Long],
//      v(1).convertTo[String],
//      v(2).convertTo[Long],
//      v(3).convertTo[Long],
//      v(4).convertTo[Double],
//      v(5).convertTo[Double],
//      v(6).convertTo[String],
//      v(7).convertTo[Double],
//      v(8).convertTo[Int],
//      v(11).convertTo[Long]
//    )
//}
