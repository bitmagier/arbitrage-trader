package org.purevalue.arbitrage.adapter.binance

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.TPDataManager.StartStreamRequest
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.binance.BinanceDataChannel.GetBinanceTradePair
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object BinanceTPDataChannel {
  def props(config: ExchangeConfig, tradePair: TradePair, binanceDataChannel: ActorRef): Props =
    Props(new BinanceTPDataChannel(config, tradePair, binanceDataChannel))
}
/**
 * Binance TradePair-based data channel
 */
class BinanceTPDataChannel(config: ExchangeConfig, tradePair: TradePair, binanceDataChannel: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BinanceTPDataChannel])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private var binanceTradePair: BinanceTradePair = _
  private var binanceTPWebSocketFlow: ActorRef = _
  private var lastUpdateId: Option[Int] = None

  private var sink: Sink[DecodedBinanceMessage, NotUsed] = _

  def createSinkTo(downstreamSink: Sink[Seq[TPStreamData], Future[Done]]): Sink[DecodedBinanceMessage, NotUsed] = {
    Flow.fromFunction(streamMapping).toMat(downstreamSink)(Keep.none)
  }

  def streamMapping(in: DecodedBinanceMessage): Seq[TPStreamData] = in match {

    case t: RawBookTickerJson =>
      Seq(t.toTicker(config.exchangeName, tradePair))

    case t: RawExtendedTickerJson =>
      Seq(t.toExtendedTicker(config.exchangeName, tradePair))

    case update: RawPartialOrderBookJson =>
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
    implicit val timeout: Timeout = AppConfig.tradeRoom.internalCommunicationTimeout
    binanceTradePair = Await.result((binanceDataChannel ? GetBinanceTradePair(tradePair)).mapTo[BinanceTradePair],
      AppConfig.tradeRoom.internalCommunicationTimeout.duration)
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
