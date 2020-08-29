package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.ExchangePublicTPDataManager.StartStreamRequest
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinancePublicDataInquirer.GetBinanceTradePair
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object BinancePublicTPDataChannel {
  def props(config: ExchangeConfig, tradePair: TradePair, binancePublicDataChannel: ActorRef): Props =
    Props(new BinancePublicTPDataChannel(config, tradePair, binancePublicDataChannel))
}
/**
 * Binance TradePair-based data channel
 * Converts Raw TradePair-based data to unified ExchangeTPStreamData
 */
class BinancePublicTPDataChannel(config: ExchangeConfig, tradePair: TradePair, binancePublicDataInquirer: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinancePublicTPDataChannel])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private var binanceTradePair: BinanceTradePair = _
  private var binanceTPWebSocketFlow: ActorRef = _
  private var lastUpdateId: Option[Int] = None

  private var sink: Sink[IncomingBinanceTradepairJson, NotUsed] = _

  def createSinkTo(downstreamSink: Sink[Seq[ExchangeTPStreamData], Future[Done]]): Sink[IncomingBinanceTradepairJson, NotUsed] = {
    Flow.fromFunction(streamMapping).toMat(downstreamSink)(Keep.none)
  }

  def streamMapping(in: IncomingBinanceTradepairJson): Seq[ExchangeTPStreamData] = in match {
    case t: RawBookTickerRestJson =>
      Seq(t.toTicker(config.exchangeName, tradePair))

    case t: RawBookTickerStreamJson =>
      Seq(t.toTicker(config.exchangeName, tradePair))

    case t: RawExtendedTickerStreamJson =>
      Seq(t.toExtendedTicker(config.exchangeName, tradePair))

    case update: RawPartialOrderBookStreamJson =>
      if (lastUpdateId.isDefined && lastUpdateId.get > update.lastUpdateId) {
        log.warn("Obsolete orderbook snapshot received")
        Seq()
      } else {
        lastUpdateId = Some(update.lastUpdateId)
        Seq(update.toOrderBookSnapshot)
      }

    case other =>
      log.error(s"unhandled: $other")
      Seq()
  }

  override def preStart() {
    log.debug(s"BinanceTPDataChannel($tradePair) initializing...")
    implicit val timeout: Timeout = Config.internalCommunicationTimeoutWhileInit
    binanceTradePair = Await.result((binancePublicDataInquirer ? GetBinanceTradePair(tradePair)).mapTo[BinanceTradePair],
      Config.internalCommunicationTimeout.duration.plus(500.millis))
    binanceTPWebSocketFlow = context.actorOf(
      BinanceTPWebSocketFlow.props(config, binanceTradePair, self), s"BinanceTPWebSocketFlow-${binanceTradePair.symbol}")
  }

  override def receive: Receive = {
    case StartStreamRequest(downstreamSink) =>
      sink = createSinkTo(downstreamSink)
      binanceTPWebSocketFlow ! BinanceTPWebSocketFlow.StartStreamRequest(sink)

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }
}
