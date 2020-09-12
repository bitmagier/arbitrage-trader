package org.purevalue.arbitrage.adapter.bitfinex

import java.time.Instant

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexOrderUpdateJson.{toOrderStatus, toOrderType}
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataInquirer.{GetBitfinexAssets, GetBitfinexTradePairs}
import org.purevalue.arbitrage.traderoom.ExchangeAccountDataManager._
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.util.HttpUtil
import org.purevalue.arbitrage.util.Util.formatDecimal
import org.purevalue.arbitrage.{traderoom, _}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsValue, JsonParser, RootJsonFormat, enrichAny}

import scala.collection.Seq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


trait IncomingBitfinexAccountJson

case class BitfinexAuthMessage(apiKey: String, authSig: String, authNonce: String, authPayload: String, filter:Array[String], event: String = "auth")
case class BitfinexOrderUpdateJson(streamType: String, // "os" = order snapshot, "on" = order new, "ou" = order update, "oc" = order cancel
                                   orderId: Long,
                                   groupId: Long,
                                   clientOrderId: Long,
                                   symbol: String,
                                   createTime: Long,
                                   updateTime: Long,
                                   amount: Double, // positive means buy, negative means sell
                                   amountOriginal: Double,
                                   orderType: String, // The type of the order: LIMIT, MARKET, STOP, STOP LIMIT, TRAILING STOP, EXCHANGE MARKET, EXCHANGE LIMIT, EXCHANGE STOP, EXCHANGE STOP LIMIT, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC.
                                   orderTypePrev: String,
                                   timeInForceTime: Long,
                                   flags: Long,
                                   orderStatus: String, // Order Status: ACTIVE, EXECUTED @ PRICE(AMOUNT) e.g. "EXECUTED @ 107.6(-0.2)", PARTIALLY FILLED @ PRICE(AMOUNT), INSUFFICIENT MARGIN was: PARTIALLY FILLED @ PRICE(AMOUNT), CANCELED, CANCELED was: PARTIALLY FILLED @ PRICE(AMOUNT), RSN_DUST (amount is less than 0.00000001), RSN_PAUSE (trading is paused / paused due to AMPL rebase event)
                                   price: Double,
                                   priceAverage: Double,
                                   priceTrailing: Double,
                                   priceAuxLimit: Double, // Auxiliary Limit price (STOP_LIMIT)
                                   notifY: Int, // 0 if false, 1 if true
                                   hidden: Int, // 1 if Hidden, 0 if not hidden
                                   placedId: Long, // if another order caused this order to be placed (OCO) this will be that other order's ID
                                   routing: String // indicates origin of action: BFX, ETHFX, API>BFX, API>ETHFX
                                  ) extends IncomingBitfinexAccountJson {
  def toOrder(exchange: String, resolveTradePair: String => TradePair): Order = traderoom.Order(
    orderId.toString,
    exchange,
    resolveTradePair(symbol),
    if (amount >= 0.0) TradeSide.Buy else TradeSide.Sell,
    toOrderType(orderType),
    price,
    if (priceAuxLimit != 0.0) Some(priceAuxLimit) else None, // TODO check what we get
    amountOriginal,
    orderStatus match {
      case s: String if s.startsWith("CANCELED") => Some(s)
      case s: String if s.startsWith("RSN_DUST") => Some(s)
      case s: String if s.startsWith("RSN_PAUSE") => Some(s)
      case s: String if s.startsWith("INSUFFICIENT MARGIN") => Some(s)
      case _ => None
    },
    Instant.ofEpochMilli(createTime),
    toOrderStatus(orderStatus),
    price,
    priceAverage,
    Instant.ofEpochMilli(updateTime)
  )

  def toOrderUpdate(exchange: String, resolveTradePair: String => TradePair): OrderUpdate = OrderUpdate(
    orderId.toString,
    resolveTradePair(symbol),
    if (amount >= 0.0) TradeSide.Buy else TradeSide.Sell,
    toOrderType(orderType),
    price,
    if (priceAuxLimit != 0.0) Some(priceAuxLimit) else None, // TODO check what we get
    Some(amountOriginal),
    Some(Instant.ofEpochMilli(createTime)),
    toOrderStatus(orderStatus),
    amount,
    priceAverage,
    Instant.ofEpochMilli(updateTime))

  def toOrderOrOrderUpdate(exchange: String, resolveTradePair: String => TradePair): ExchangeAccountStreamData = {
    if (streamType == "on") toOrder(exchange, resolveTradePair)
    else toOrderUpdate(exchange, resolveTradePair)
  }
}
object BitfinexOrderUpdateJson {

  import DefaultJsonProtocol._

  def apply(streamType: String, v: Array[JsValue]): BitfinexOrderUpdateJson = BitfinexOrderUpdateJson(
    streamType,
    v(0).convertTo[Long],
    v(1).convertTo[Long],
    v(2).convertTo[Long],
    v(3).convertTo[String],
    v(4).convertTo[Long],
    v(5).convertTo[Long],
    v(6).convertTo[Double],
    v(7).convertTo[Double],
    v(8).convertTo[String],
    v(9).convertTo[String],
    v(10).convertTo[Long],
    v(11).convertTo[Long],
    v(12).convertTo[String],
    v(13).convertTo[Double],
    v(14).convertTo[Double],
    v(15).convertTo[Double],
    v(16).convertTo[Double],
    v(17).convertTo[Int],
    v(18).convertTo[Int],
    v(19).convertTo[Long],
    v(20).convertTo[String]
  )

  def toOrderType(orderType: String): OrderType = orderType match {
    case "LIMIT" => OrderType.LIMIT
    case "MARKET" => OrderType.MARKET
    case "STOP" => OrderType.STOP_LOSS
    case "STOP LIMIT" => OrderType.STOP_LOSS_LIMIT
    case "EXCHANGE MARKET" => OrderType.MARKET
    case "EXCHANGE LIMIT" => OrderType.LIMIT
    case "EXCHANGE STOP" => OrderType.STOP_LOSS
    case "EXCHANGE STOP LIMIT" => OrderType.STOP_LOSS_LIMIT
    // not implemented, because not used: TRAILING STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC.
    case unhandled: String => throw new RuntimeException(s"bitfinex unhandled ORDER_TYPE $unhandled")
  }


  def toOrderStatus(orderStatus: String): OrderStatus = orderStatus match {
    case "ACTIVE" => OrderStatus.NEW
    case "CANCELED" => OrderStatus.CANCELED
    case s: String if s.startsWith("EXECUTED") => OrderStatus.FILLED
    case s: String if s.startsWith("PARTIALLY FILLED") => OrderStatus.PARTIALLY_FILLED
    case s: String if s.startsWith("RSN_DUST") => OrderStatus.REJECTED
    case s: String if s.startsWith("INSUFFICIENT MARGIN") => OrderStatus.REJECTED
  }
}

case class BitfinexTradeExecutedJson(tradeId: Long,
                                     clientOrderId: Long,
                                     symbol: String,
                                     executionTime: Long,
                                     orderId: Long,
                                     execAmount: Double, // positive means buy, negative means sell
                                     execPrice: Double,
                                     orderType: String,
                                     orderPrice: Double,
                                     maker: Int, // 1 if true, -1 if false
                                     fee: Double, // "tu" only
                                     feeCurrency: String) extends IncomingBitfinexAccountJson {
  def toOrderUpdate(exchangeName: String, resolveTradePair: String => BitfinexTradePair): OrderUpdate = traderoom.OrderUpdate(
    orderId.toString,
    resolveTradePair(symbol),
    if (execAmount >= 0.0) TradeSide.Buy else TradeSide.Sell,
    toOrderType(orderType),
    orderPrice,
    None,
    None,
    None,
    OrderStatus.FILLED,
    execAmount,
    orderPrice,
    Instant.ofEpochMilli(executionTime)
  )
}
object BitfinexTradeExecutedJson {

  import DefaultJsonProtocol._

  def apply(v: Array[JsValue]): BitfinexTradeExecutedJson =
    BitfinexTradeExecutedJson(
      v(0).convertTo[Long],
      v(1).convertTo[Long],
      v(2).convertTo[String],
      v(3).convertTo[Long],
      v(4).convertTo[Long],
      v(5).convertTo[Double],
      v(6).convertTo[Double],
      v(7).convertTo[String],
      v(8).convertTo[Double],
      v(9).convertTo[Int],
      v(10).convertTo[Double],
      v(11).convertTo[String])
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

case class SubmitLimitOrderJson(`type`: String, symbol: String, price: String, amount: String, meta: String)

case class SingleOrderResponseJson(id: Long,
                                   gid: Long,
                                   cid: Long,
                                   symbol: String,
                                   mtsCreate: Long,
                                   orderStatus: String,
                                   routing: String)
case class OrderResponseJson(mts: Long,
                             orders: List[SingleOrderResponseJson],
                             status: String, // SUCCESS, ERROR. FAILURE, ...
                             text: String)

object BitfinexAccountDataJsonProtocoll extends DefaultJsonProtocol {
  implicit val bitfinexAuthMessage: RootJsonFormat[BitfinexAuthMessage] = jsonFormat6(BitfinexAuthMessage)
  implicit val submitLimitOrderJson: RootJsonFormat[SubmitLimitOrderJson] = jsonFormat5(SubmitLimitOrderJson)

  implicit object singleOrderResponseJson extends RootJsonFormat[SingleOrderResponseJson] {
    def read(value: JsValue): SingleOrderResponseJson = {
      val v = value.convertTo[Array[JsValue]]
      SingleOrderResponseJson(
        v(0).convertTo[Long],
        v(1).convertTo[Long],
        v(2).convertTo[Long],
        v(3).convertTo[String],
        v(4).convertTo[Long],
        v(11).convertTo[String],
        v(18).convertTo[String])
    }

    def write(v: SingleOrderResponseJson): JsValue = throw new NotImplementedError
  }
  implicit object submitOrderResponseJson extends RootJsonFormat[OrderResponseJson] {
    def read(value: JsValue): OrderResponseJson = {
      val v = value.convertTo[Array[JsValue]]
      OrderResponseJson(v(0).convertTo[Long], v(4).convertTo[List[SingleOrderResponseJson]], v(6).convertTo[String], v(7).convertTo[String])
    }

    def write(v: OrderResponseJson): JsValue = throw new NotImplementedError
  }
}


object BitfinexAccountDataChannel {
  def props(config: ExchangeConfig,exchangePublicDataInquirer: ActorRef): Props =
    Props(new BitfinexAccountDataChannel(config, exchangePublicDataInquirer))
}
class BitfinexAccountDataChannel(config: ExchangeConfig,
                                 exchangePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexAccountDataChannel])

  val BaseRestEndpoint = "https://api.bitfinex.com"
  val WebSocketEndpoint: Uri = Uri("wss://api.bitfinex.com/ws/2")

  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var bitfinexTradePairs: Set[BitfinexTradePair] = _
  var bitfinexAssets: Set[BitfinexSymbol] = _

  def tradePairToApiSymbol(tp: TradePair): String = bitfinexTradePairs.find(e => e.baseAsset == tp.baseAsset && e.quoteAsset == tp.quoteAsset) match {
    case Some(tp) => tp.apiSymbol
    case None => throw new IllegalArgumentException(s"unable to find bitfinex tradepair: $tp. Available are: $bitfinexTradePairs")
  }

  def symbolToAsset(symbol:String): Asset = bitfinexAssets.find(_.apiSymbol == symbol) match {
    case Some(bitfinexAsset) => bitfinexAsset.asset
    case None => StaticConfig.AllAssets.find(_._1 == symbol) match {
      case Some(globalAsset) => globalAsset._2
      case None => throw new IllegalArgumentException(s"unable to find Asset for bitfinex symbol $symbol. Bitfinex has: $bitfinexAssets")
    }
  }

  import BitfinexAccountDataJsonProtocoll._

  val downStreamFlow: Flow[Seq[IncomingBitfinexAccountJson], Seq[ExchangeAccountStreamData], NotUsed] = Flow.fromFunction {
    data: Seq[IncomingBitfinexAccountJson] =>
      data.map {
        // @formatter:off
        case o: BitfinexOrderUpdateJson   => o.toOrderOrOrderUpdate(config.exchangeName, symbol => bitfinexTradePairs.find(_.apiSymbol == symbol).get)
        case t: BitfinexTradeExecutedJson => t.toOrderUpdate(config.exchangeName, symbol => bitfinexTradePairs.find(_.apiSymbol == symbol).get)
        case w: BitfinexWalletUpdateJson  => w.toWalletAssetUpdate(symbol => symbolToAsset(symbol))
        case x                            => log.debug(s"$x"); throw new NotImplementedError
        // @formatter:on
      }
  }

  def decodeJsonObject(s: String): Unit = {
    JsonParser(s).asJsObject match {
      case j: JsObject if j.fields.contains("event") =>
        j.fields("event").convertTo[String] match {
          case "auth" if j.fields("status").convertTo[String] == "OK" =>
            log.trace(s"received auth response: $j")
            None
          case "auth" =>
            throw new RuntimeException(s"bitfinex account WebSocket authentification failed with: $j")
          case _ =>
            log.info(s"bitfinex: watching event message: $s")
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
          case streamType: String if Seq("os", "on", "ou", "oc").contains(streamType) =>
            if (log.isTraceEnabled) log.trace(s"received event 'order update': $s")
            dataArray(2).convertTo[Array[JsObject]].map(e => BitfinexOrderUpdateJson(streamType, e.convertTo[Array[JsValue]]))
          case streamType: String if Seq("ps", "pn", "pu", "pc").contains(streamType) =>
            log.debug(s"ignoring 'Position' data: $s")
            Seq()
          case "tu" =>
            if (log.isTraceEnabled) log.trace(s"ignoring trade update event: $s")
            Seq() // ignoring trade updates (prefering order updates, which have more details)
          case "te" =>
            if (log.isTraceEnabled) log.trace(s"received event 'trade executed': $s") // TODO maybe we can ignore trade events because we have order events
            Seq(BitfinexTradeExecutedJson(dataArray(2).convertTo[Array[JsValue]]))
          case "ws" =>
            if (log.isTraceEnabled) log.trace(s"received event 'wallet snapshot': $s")
            dataArray(2).convertTo[Array[JsValue]].map(e => BitfinexWalletUpdateJson(e.convertTo[Array[JsValue]]))
          case "wu" =>
            if (log.isTraceEnabled) log.trace(s"received event 'wallet update': $s")
            Seq(BitfinexWalletUpdateJson(dataArray(2).convertTo[Array[JsValue]]))
          case "bu" =>
            if (log.isTraceEnabled) log.trace(s"watching event 'balance update': $s")
            Seq() // we ignore that event, we can calculate balance from wallet
          case "miu" => Seq() // ignore margin info update
          case "fiu" => Seq() // ignore funding info
          case s: String if Seq("fte", "ftu").contains(s) => Seq() // ignore funding trades
          case "n" =>
            if (log.isTraceEnabled) log.trace(s"received notification event: $s")
            Seq() // ignore notifications
          case x => throw new RuntimeException(s"bitfinex: received data of unidentified stream type '$x': $s")
        }
    }
  }

  val wsFlow: Flow[Message, Seq[IncomingBitfinexAccountJson], NotUsed] = Flow.fromFunction {
    case msg: TextMessage =>
      val f: Future[Seq[IncomingBitfinexAccountJson]] = {
        msg.toStrict(Config.httpTimeout)
          .map(_.getStrictText)
          .map {
            case s: String if s.startsWith("{") => decodeJsonObject(s); Nil
            case s: String if s.startsWith("[") => decodeDataArray(s)
            case x => throw new RuntimeException(s"unidentified response: $x")
          }
      }
      try {
        Await.result(f, Config.httpTimeout.plus(1000.millis))
      } catch {
        case e: Exception => throw new RuntimeException(s"While decoding WebSocket stream event: $msg", e)
      }
    case _ =>
      log.warn(s"Received non TextMessage")
      Nil
  }

  def authMessage: BitfinexAuthMessage = {
    val authNonce: String = Instant.now.toEpochMilli.toString
    val authPayload = "AUTH" + authNonce
    BitfinexAuthMessage(
      apiKey = config.secrets.apiKey,
      HttpUtil.hmacSha384Signature(authPayload, config.secrets.apiSecretKey),
      authNonce,
      authPayload,
      filter = Array("trading","trading-tBTCUSD","wallet","wallet-exchange-BTC","balance","notify")
    )
  }

  def createFlowTo(sink: Sink[Seq[ExchangeAccountStreamData], Future[Done]]): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      wsFlow
        .via(downStreamFlow)
        .toMat(sink)(Keep.right),
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
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    bitfinexTradePairs = Await.result(
      (exchangePublicDataInquirer ? GetBitfinexTradePairs()).mapTo[Set[BitfinexTradePair]],
      timeout.duration.plus(500.millis)
    )
  }


  def pullBitfinexAssets(): Unit = {
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    bitfinexAssets = Await.result(
      (exchangePublicDataInquirer ? GetBitfinexAssets()).mapTo[Set[BitfinexSymbol]],
      timeout.duration.plus(500.millis)
    )
  }

  override def preStart(): Unit = {
    pullBitfinexTradePairs()
    pullBitfinexAssets()
  }

  def connect(sink: Sink[Seq[ExchangeAccountStreamData], Future[Done]]): Unit = {
    if (log.isTraceEnabled) log.trace("starting WebSocket stream")
    ws = Http().singleWebSocketRequest(WebSocketRequest(WebSocketEndpoint), createFlowTo(sink))
    connected = createConnected
  }

  def toSubmitLimitOrderJson(o: OrderRequest, resolveSymbol: TradePair => String, affiliateCode: Option[String]): SubmitLimitOrderJson =
    SubmitLimitOrderJson(
      "LIMIT",
      resolveSymbol.apply(o.tradePair),
      formatDecimal(o.limit),
      formatDecimal(o.amountBaseAsset),
      affiliateCode.map(code => s"""{aff_code: "$code"}""").getOrElse("{}")
    )

  def newLimitOrder(o: OrderRequest): Future[NewOrderAck] = {
    import BitfinexAccountDataJsonProtocoll._

    HttpUtil.httpRequestJsonBitfinexAccount[OrderResponseJson](
      HttpMethods.POST,
      s"$BaseRestEndpoint/v2/auth/w/order/submit",
      Some(toSubmitLimitOrderJson(o, tp => tradePairToApiSymbol(tp), config.refCode).toJson.compactPrint),
      config.secrets
    ).map {
      case r: OrderResponseJson if r.status == "SUCCESS" && r.orders.length == 1 =>
        NewOrderAck(config.exchangeName, o.tradePair, r.orders.head.id.toString, o.id)
      case r: OrderResponseJson =>
        throw new RuntimeException(s"Something went wrong with placing our limit-order: $r")
    }
  }

  def cancelOrder(tradePair: TradePair, externalOrderId: Long): Future[CancelOrderResult] = {
    import BitfinexAccountDataJsonProtocoll._

    HttpUtil.httpRequestJsonBitfinexAccount[OrderResponseJson](
      HttpMethods.POST,
      s"$BaseRestEndpoint/v2/auth/w/order/cancel",
      Some(s"{id:$externalOrderId}"),
      config.secrets
    ).map {
      case r: OrderResponseJson if r.status == "SUCCESS" && r.orders.size == 1 =>
        CancelOrderResult(tradePair, r.orders.head.id.toString, success = true)
      case r: OrderResponseJson =>
        log.debug(s"Cancel order failed. Result: $r")
        CancelOrderResult(tradePair, externalOrderId.toString, success = false)
    }
  }

  override def receive: Receive = {
    case StartStreamRequest(sink) =>
      connect(sink)
      sender() ! Initialized()
    // Messages from ExchangeAccountDataManager (forwarded from TradeRoom-LiquidityManager or TradeRoom-OrderExecutionManager)
    case CancelOrder(tradePair, externalOrderId) => cancelOrder(tradePair, externalOrderId.toLong).pipeTo(sender())
    case NewLimitOrder(o) => newLimitOrder(o).pipeTo(sender())
  }
}
