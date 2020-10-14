package org.purevalue.arbitrage.adapter.coinbase

import java.time.Instant
import java.util.UUID

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.http.scaladsl.model.{HttpMethods, HttpResponse, StatusCodes}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import org.purevalue.arbitrage.adapter.coinbase.CoinbaseAccountDataChannel.{Connect, OnStreamsRunning}
import org.purevalue.arbitrage.adapter.coinbase.CoinbaseHttpUtil.{Signature, httpRequestCoinbaseAccount, httpRequestJsonCoinbaseAccount, parseServerTime}
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataChannel.CoinbaseWebSocketEndpoint
import org.purevalue.arbitrage.adapter.coinbase.CoinbasePublicDataInquirer.{CoinbaseBaseRestEndpoint, DeliverAccounts, GetCoinbaseTradePairs}
import org.purevalue.arbitrage.traderoom.TradeRoom.OrderRef
import org.purevalue.arbitrage.traderoom._
import org.purevalue.arbitrage.traderoom.exchange.Exchange._
import org.purevalue.arbitrage.traderoom.exchange.{Balance, CompleteWalletUpdate, ExchangeAccountStreamData}
import org.purevalue.arbitrage.util.Util.{alignToStepSizeCeil, alignToStepSizeNearest, formatDecimalWithFixPrecision}
import org.purevalue.arbitrage.util.{HttpUtil, RestartIntentionException}
import org.purevalue.arbitrage.{ExchangeConfig, GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, JsObject, JsonParser, RootJsonFormat, enrichAny}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}


private[coinbase] case class SubscribeRequestWithAuthJson(`type`: String = "subscribe",
                                                          product_ids: Seq[String],
                                                          channels: Seq[String],
                                                          key: String,
                                                          signature: String,
                                                          timestamp: String,
                                                          passphrase: String)

private[coinbase] trait IncomingCoinbaseAccountJson

private[coinbase] object CoinbaseOrder {
  def toOrderType(order_type: String): OrderType = order_type match {
    case "limit" => OrderType.LIMIT
    case "market" => OrderType.MARKET
    case "stop" => OrderType.STOP_LOSS_LIMIT
    case _ => throw new NotImplementedError()
  }

  def toString(t: OrderType): String = t match {
    case OrderType.LIMIT => "limit"
    case OrderType.MARKET => "market"
    case _ => throw new NotImplementedError()
  }

  def toTradeSide(side: String): TradeSide = side match {
    case "buy" => TradeSide.Buy
    case "sell" => TradeSide.Sell
    case _ => throw new NotImplementedError()
  }

  def toString(side: TradeSide): String = side match {
    case TradeSide.Buy => "buy"
    case TradeSide.Sell => "sell"
  }

  def toOrderStatus(status: String): OrderStatus = status match {
    case "open" => OrderStatus.NEW
    case "pending" => OrderStatus.NEW
    case "active" => OrderStatus.PARTIALLY_FILLED
    case "done" => OrderStatus.FILLED
  }
}
// A valid order has been received and is now active.
// This message is emitted for every single valid order as soon as the matching engine receives it whether it fills immediately or not.
private[coinbase] case class CoinbaseOrderReceivedJson(`type`: String,
                                                       time: String, // "2014-11-07T08:19:27.028459Z",
                                                       product_id: String, //"BTC-USD",
                                                       sequence: Long, // 10,
                                                       order_id: String, // "d50ec984-77a8-460a-b958-66f114b0de9b",
                                                       size: Option[String], // "1.34", (not delivered for "market" orders)
                                                       price: Option[String], // "502.1", (not delivered for "market" orders)
                                                       side: String, // "buy",
                                                       order_type: String // "limit"
                                                      ) extends IncomingCoinbaseAccountJson {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = {
    val ts = Instant.parse(time)
    OrderUpdate(
      order_id,
      exchange,
      resolveProductId(product_id),
      CoinbaseOrder.toTradeSide(side),
      Some(CoinbaseOrder.toOrderType(order_type)),
      price.map(_.toDouble),
      None, // ?
      size.map(_.toDouble),
      Some(ts),
      Some(OrderStatus.NEW),
      None,
      None,
      None,
      ts
    )
  }
}

private[coinbase] case class CoinbaseOrderChangedJson(`type`: String,
                                                      time: String,
                                                      sequence: Long,
                                                      order_id: String,
                                                      product_id: String,
                                                      new_size: String,
                                                      old_size: String,
                                                      price: String,
                                                      side: String) extends IncomingCoinbaseAccountJson {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = OrderUpdate(
    order_id,
    exchange,
    resolveProductId(product_id),
    CoinbaseOrder.toTradeSide(side),
    None,
    Some(price.toDouble),
    None,
    Some(new_size.toDouble),
    None,
    None,
    None,
    None,
    None,
    Instant.parse(time)
  )
}

private[coinbase] case class CoinbaseOrderDoneJson(`type`: String,
                                                   time: String,
                                                   product_id: String,
                                                   sequence: Long,
                                                   price: String,
                                                   order_id: String,
                                                   reason: String, // "filled" or "canceled"
                                                   side: String,
                                                   remaining_size: String) extends IncomingCoinbaseAccountJson {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = OrderUpdate(
    order_id,
    exchange,
    resolveProductId(product_id),
    CoinbaseOrder.toTradeSide(side),
    None,
    Some(price.toDouble),
    None,
    None,
    None,
    reason match {
      case "filled" => Some(OrderStatus.FILLED)
      case "canceled" => Some(OrderStatus.CANCELED)
    },
    None,
    Some(remaining_size.toDouble),
    None,
    Instant.parse(time)
  )
}

private[coinbase] case class NewOrderRequestJson(client_oid: String,
                                                 product_id: String,
                                                 `type`: String, // "limit" or "market"
                                                 side: String,
                                                 size: String,
                                                 price: String
                                                ) // not needed: stop, stop_price

private[coinbase] case class NewOrderResponseJson(id: String,
                                                  product_id: String,
                                                  settled: Boolean,
                                                  price: String,
                                                  size: String,
                                                  side: String,
                                                  stp: String,
                                                  `type`: String,
                                                  time_in_force: String,
                                                  post_only: Boolean,
                                                  created_at: String,
                                                  fill_fees: String,
                                                  filled_size: String,
                                                  executed_value: String,
                                                  status: String // "pending"
                                                 ) {
  def toOrderUpdate(exchange: String, resolveProductId: String => TradePair): OrderUpdate = {
    val ts = Instant.parse(created_at)
    OrderUpdate(
      id,
      exchange,
      resolveProductId(product_id),
      CoinbaseOrder.toTradeSide(side),
      Some(CoinbaseOrder.toOrderType(`type`)),
      Some(price.toDouble),
      None,
      Some(size.toDouble),
      Some(ts),
      Some(CoinbaseOrder.toOrderStatus(status)),
      Some(filled_size.toDouble),
      None,
      Some(executed_value.toDouble / filled_size.toDouble),
      ts
    )
  }

  def toNewOrderAck(exchange: String, resolveProductId: String => TradePair, ourOrderId: UUID): NewOrderAck =
    NewOrderAck(
      exchange,
      resolveProductId(product_id),
      id,
      ourOrderId
    )
}

private[coinbase] case class CoinbaseAccountJson(id: String,
                                                 currency: String,
                                                 balance: String,
                                                 available: String,
                                                 hold: String,
                                                 profile_id: String,
                                                 trading_enabled: Boolean) {
  def toBalance: Balance = Balance(
    Asset(currency),
    available.toDouble,
    hold.toDouble
  )
}

private[coinbase] object CoinbaseAccountJsonProtocol extends DefaultJsonProtocol {
  implicit val subscribeRequestWithAuthJson: RootJsonFormat[SubscribeRequestWithAuthJson] = jsonFormat7(SubscribeRequestWithAuthJson)
  implicit val coinbaseOrderReceivedJson: RootJsonFormat[CoinbaseOrderReceivedJson] = jsonFormat9(CoinbaseOrderReceivedJson)
  implicit val coinbaseOrderChangedJson: RootJsonFormat[CoinbaseOrderChangedJson] = jsonFormat9(CoinbaseOrderChangedJson)
  implicit val coinbaseOrderDoneJson: RootJsonFormat[CoinbaseOrderDoneJson] = jsonFormat9(CoinbaseOrderDoneJson)
  implicit val newOrderRequestJson: RootJsonFormat[NewOrderRequestJson] = jsonFormat6(NewOrderRequestJson)
  implicit val newOrderResponseJson: RootJsonFormat[NewOrderResponseJson] = jsonFormat15(NewOrderResponseJson)
  implicit val coinbaseAccountJson: RootJsonFormat[CoinbaseAccountJson] = jsonFormat7(CoinbaseAccountJson)
}

object CoinbaseAccountDataChannel {
  private case class Connect()
  private case class OnStreamsRunning()

  def props(globalConfig: GlobalConfig,
            exchangeConfig: ExchangeConfig,
            exchange: ActorRef,
            exchangePublicDataInquirer: ActorRef): Props = Props(new CoinbaseAccountDataChannel(globalConfig, exchangeConfig, exchange, exchangePublicDataInquirer))
}
private[coinbase] class CoinbaseAccountDataChannel(globalConfig: GlobalConfig,
                                                   exchangeConfig: ExchangeConfig,
                                                   exchange: ActorRef,
                                                   exchangePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[CoinbaseAccountDataChannel])

  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  val DeliverAccountsSchedule: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(1.second, 1500.millis, self, DeliverAccounts())

  val UserChannelName: String = "user"

  var coinbaseTradePairsByProductId: Map[String, CoinbaseTradePair] = _
  var coinbaseTradePairsByTradePair: Map[TradePair, CoinbaseTradePair] = _

  import CoinbaseAccountJsonProtocol._

  def pullCoinbaseTradePairs(): Unit = {
    implicit val timeout: Timeout = globalConfig.internalCommunicationTimeoutDuringInit
    val coinbaseTradePairs: Set[CoinbaseTradePair] = Await.result(
      (exchangePublicDataInquirer ? GetCoinbaseTradePairs()).mapTo[Set[CoinbaseTradePair]],
      timeout.duration.plus(500.millis))

    coinbaseTradePairsByProductId = coinbaseTradePairs.map(e => e.id -> e).toMap
    coinbaseTradePairsByTradePair = coinbaseTradePairs.map(e => e.toTradePair -> e).toMap
  }

  // @formatter:off
  def exchangeDataMapping(in: IncomingCoinbaseAccountJson): ExchangeAccountStreamData = in match {
    case o: CoinbaseOrderReceivedJson => o.toOrderUpdate(exchangeConfig.name, id => coinbaseTradePairsByProductId(id).toTradePair)
    case o: CoinbaseOrderChangedJson  => o.toOrderUpdate(exchangeConfig.name, id => coinbaseTradePairsByProductId(id).toTradePair)
    case o: CoinbaseOrderDoneJson     => o.toOrderUpdate(exchangeConfig.name, id => coinbaseTradePairsByProductId(id).toTradePair)
  } // @formatter:on

  // @formatter:off
  def decodeJsObject(messageType: String, j: JsObject): Seq[IncomingCoinbaseAccountJson] = {
    messageType match {
      case "subscriptions" =>
        if (log.isTraceEnabled) log.trace(j.compactPrint)
        self ! OnStreamsRunning()
        Nil
      case "received"      => Seq(j.convertTo[CoinbaseOrderReceivedJson])
      case "change"        => Seq(j.convertTo[CoinbaseOrderChangedJson]) // An order has changed. This is the result of self-trade prevention adjusting the order size or available funds
      case "done"          => Seq(j.convertTo[CoinbaseOrderDoneJson])
      case "open"          => Nil // ignore: This message will only be sent for orders which are not fully filled immediately.
      case "match"         => Nil // ignore: A trade occurred between two orders.
      case "activate"      => Nil // ignore: An activate message is sent when a stop order is placed.
      case "error"         => throw new RuntimeException(j.prettyPrint)
      case other           => log.warn(s"received unhandled message type: $j"); Nil
    }
  } // // @formatter:on

  def decodeMessage(message: Message): Future[Seq[IncomingCoinbaseAccountJson]] = message match {
    case msg: TextMessage =>
      msg.toStrict(globalConfig.httpTimeout)
        .map(_.getStrictText)
        .map(s => JsonParser(s).asJsObject() match {
          case j: JsObject if j.fields.contains("type") =>
            import DefaultJsonProtocol._
            decodeJsObject(j.fields("type").convertTo[String], j)
          case j: JsObject =>
            log.warn(s"Unknown json object received: $j")
            Nil
        })
    case _ =>
      log.warn(s"Received non TextMessage")
      Future.successful(Nil)
  }

  // coinbase allows a maximum diff of 30 seconds between their server time and the timestamp value in the request
  def queryServerTime(): Future[Double] = {
    // the returned format from coinbase is not always JSON-compatible (no zeros after decimal point at the full second
    // so we parse it manually
    HttpUtil.httpGet(s"$CoinbaseBaseRestEndpoint/time") flatMap {
      case HttpResponse(code, headers, entity, protocol) =>
        (code,
          entity.toStrict(globalConfig.httpTimeout)
            .map(_.data.utf8String)) match {
          case (code, entity) if code.isSuccess() => entity.map(parseServerTime)
          case (code, entity) => throw new RuntimeException(s"coinbase: GET /time failed with: $code, $entity")
        }
    }
  }

  def subscribeMessage: SubscribeRequestWithAuthJson = {
    Await.result(
      queryServerTime().map { serverTime =>
        val s: Signature = CoinbaseHttpUtil.createSignature(HttpMethods.GET, s"$CoinbaseBaseRestEndpoint/users/self/verify", None, exchangeConfig.secrets, serverTime)
        SubscribeRequestWithAuthJson("subscribe", coinbaseTradePairsByProductId.keys.toSeq, Seq(UserChannelName), s.cbAccessKey, s.cbAccessSign, s.cbAccessTimestamp, s.cbAccessPassphrase)
      },
      globalConfig.httpTimeout.plus(1.second))
  }

  def wsFlow(): Flow[Message, Message, Promise[Option[Message]]] = {
    Flow.fromSinkAndSourceCoupledMat(
      Sink.foreach[Message](message =>
        decodeMessage(message)
          .map(_.map(exchangeDataMapping))
          .map(IncomingAccountData)
          .pipeTo(exchange)
      ),
      Source(
        List(TextMessage(subscribeMessage.toJson.compactPrint))
      ).concatMat(Source.maybe[Message])(Keep.right))(Keep.right)
  }

  var ws: (Future[WebSocketUpgradeResponse], Promise[Option[Message]]) = _
  var connected: Future[Done.type] = _

  def createConnected: Future[Done.type] =
    ws._1.flatMap { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        log.info("connected")
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }


  def connect(): Unit = {
    log.info(s"starting WebSocket $CoinbaseWebSocketEndpoint ...")
    ws = Http().singleWebSocketRequest(WebSocketRequest(CoinbaseWebSocketEndpoint), wsFlow())
    ws._2.future.onComplete { e =>
      log.info(s"connection closed: ${e.get}")
      throw new RestartIntentionException(s"coinbase account connection lost") // trigger restart
    }
    connected = createConnected
  }

  def onStreamsRunning(): Unit = {
    exchange ! AccountDataChannelInitialized()
  }


  def newLimitOrder(o: OrderRequest): Future[NewOrderAck] = {

    def newLimitOrder(o: OrderRequest, serverTime: Double): Future[NewOrderAck] = {
      val coinbaseTradepair = coinbaseTradePairsByTradePair(o.tradePair)

      val size: String = formatDecimalWithFixPrecision(
        Math.max(coinbaseTradepair.baseMinSize,
          alignToStepSizeCeil(o.amountBaseAsset, coinbaseTradepair.baseIncrement)), 8) // The size must be greater than the base_min_size for the product and no larger than the base_max_size

      val price: String = formatDecimalWithFixPrecision(
        alignToStepSizeNearest(o.limit, coinbaseTradepair.quoteIncrement), 8) // The price must be specified in quote_increment product units.

      val requestBody: String = NewOrderRequestJson(
        o.id.toString,
        coinbaseTradePairsByTradePair(o.tradePair).id,
        CoinbaseOrder.toString(OrderType.LIMIT),
        CoinbaseOrder.toString(o.tradeSide),
        size,
        price).toJson.compactPrint

      httpRequestJsonCoinbaseAccount[NewOrderResponseJson, String](HttpMethods.POST, s"$CoinbaseBaseRestEndpoint/orders", Some(requestBody), exchangeConfig.secrets, serverTime)
        .map {
          case Left(newOrderResponse) =>
            exchange ! IncomingAccountData(Seq(newOrderResponse.toOrderUpdate(exchangeConfig.name, id => coinbaseTradePairsByProductId(id).toTradePair)))
            newOrderResponse.toNewOrderAck(exchangeConfig.name, id => coinbaseTradePairsByProductId(id).toTradePair, o.id)
          case Right(error) => throw new RuntimeException(s"newLimitOrder failed: $error")
        } recover {
        case e: Exception =>
          log.error(s"NewLimitOrder failed. Request body:\n$requestBody\ncoinbaseTradePair:$coinbaseTradepair\n", e)
          throw e
      }
    }

    for {
      serverTime <- queryServerTime()
      newOrderAck <- newLimitOrder(o, serverTime)
    } yield newOrderAck
  }

  def cancelOrder(ref: OrderRef): Future[CancelOrderResult] = {

    def cancelOrder(ref: OrderRef, serverTime: Double): Future[CancelOrderResult] = {
      val productId: String = coinbaseTradePairsByTradePair(ref.tradePair).id
      val uri = s"$CoinbaseBaseRestEndpoint/orders/${ref.externalOrderId}?product_id=$productId"
      httpRequestCoinbaseAccount(
        HttpMethods.DELETE,
        uri,
        None,
        exchangeConfig.secrets,
        serverTime
      ) map {
        case (statusCode, j) if statusCode.isSuccess() => CancelOrderResult(exchangeConfig.name, ref.tradePair, productId, success = true, orderUnknown = false, Some(s"HTTP-$statusCode $j"))
        case (statusCode, j) =>
          log.warn(s"DELETE $uri failed with: $statusCode, $j")
          CancelOrderResult(exchangeConfig.name, ref.tradePair, productId, success = false, orderUnknown = true, Some(s"HTTP-$statusCode $j")) // TODO decode error message to check if reason = Order unknown. For now we always say orderUnknown=true here
      }
    }

    for {
      serverTime <- queryServerTime()
      cancelOrderResult <- cancelOrder(ref: OrderRef, serverTime)
    } yield cancelOrderResult
  }

  def deliverAccounts(): Unit = {

    def queryAccounts(serverTime: Double): Future[IncomingAccountData] = {
      httpRequestJsonCoinbaseAccount[Seq[CoinbaseAccountJson], JsObject](HttpMethods.GET, s"$CoinbaseBaseRestEndpoint/accounts", None, exchangeConfig.secrets, serverTime)
        .map {
          case Left(accounts) =>
            Seq(CompleteWalletUpdate(
              accounts
                .map(_.toBalance)
                .map(e => e.asset -> e)
                .toMap
            ))
          case Right(error) =>
            log.warn(s"coinbase: queryAccounts() failed: $error")
            Nil
        }
        .map(e => IncomingAccountData(e))
    }

    (for {
      serverTime <- queryServerTime()
      accounts <- queryAccounts(serverTime)
    } yield accounts)
      .pipeTo(exchange)
  }

  override def postStop(): Unit = {
    if (ws != null && !ws._2.isCompleted) ws._2.success(None)
  }

  override def preStart(): Unit = {
    try {
      pullCoinbaseTradePairs()
      self ! Connect()
    } catch {
      case e: Exception => log.error("preStart failed", e)
    }
  }

  // @formatter:off
  override def receive: Receive = {
    case Connect()                               => connect()
    case OnStreamsRunning()                      => onStreamsRunning()
    case NewLimitOrder(o)                        => newLimitOrder(o).pipeTo(sender())
    case CancelOrder(ref)                        => cancelOrder(ref).pipeTo(sender())
    case DeliverAccounts()                       => deliverAccounts()
    case Status.Failure(e)                       => log.error("failure", e)
  }
  // @formatter:on
}
