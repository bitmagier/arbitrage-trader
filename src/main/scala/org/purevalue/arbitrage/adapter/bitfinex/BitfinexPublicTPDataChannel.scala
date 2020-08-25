package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.purevalue.arbitrage.TPDataManager.StartStreamRequest
import org.purevalue.arbitrage._
import org.purevalue.arbitrage.adapter.bitfinex.BitfinexPublicDataChannel.GetBitfinexTradePair
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContextExecutor, Future}


object BitfinexPublicTPDataChannel {
  def props(config: ExchangeConfig, tradePair: TradePair, bitfinexDataChannel: ActorRef): Props =
    Props(new BitfinexPublicTPDataChannel(config, tradePair, bitfinexDataChannel))
}

/**
 * Bitfinex TradePair-based data channel
 * Converts Raw TradePair-based data to unified ExchangeTPStreamData
 */
class BitfinexPublicTPDataChannel(config: ExchangeConfig, tradePair: TradePair, bitfinexDataChannel:ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[BitfinexPublicTPDataChannel])
  implicit val system: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private var bitfinexTradePair: BitfinexTradePair = _
  private var bitfinexTPWebSocketFlow: ActorRef = _

  private var sink: Sink[DecodedBitfinexMessage, NotUsed] = _

  def createSinkTo(downstreamSink: Sink[Seq[ExchangeTPStreamData], Future[Done]]): Sink[DecodedBitfinexMessage, NotUsed] = {
    Flow.fromFunction(streamMapping).toMat(downstreamSink)(Keep.none)
  }

  def streamMapping(in: DecodedBitfinexMessage): Seq[ExchangeTPStreamData] = in match {
    case t: RawTickerJson =>
      Seq(t.value.toTicker(config.exchangeName, tradePair))

    case o: RawOrderBookSnapshotJson =>
      Seq(o.toOrderBookSnapshot)

    case o: RawOrderBookUpdateJson =>
      Seq(o.toOrderBookUpdate)

    case h: Heartbeat =>
      Seq()

    case other =>
      log.error(s"unhandled: $other")
      Seq()
  }

  override def preStart() {
    log.debug(s"BitfinexTradePairDataStreamer($tradePair) initializing...")
    implicit val timeout: Timeout = Config.internalCommunicationTimeout
    bitfinexTradePair = Await.result((bitfinexDataChannel ? GetBitfinexTradePair(tradePair)).mapTo[BitfinexTradePair],
      Config.internalCommunicationTimeout.duration)
    bitfinexTPWebSocketFlow = context.actorOf(
      BitfinexTPWebSocketFlow.props(config, bitfinexTradePair, self), s"BitfinexTPWebSocketFlow-${bitfinexTradePair.apiSymbol}")
  }

  override def receive: Receive = {

    case StartStreamRequest(downstreamSink) =>
      sink = createSinkTo(downstreamSink)
      bitfinexTPWebSocketFlow ! BitfinexTPWebSocketFlow.StartStreamRequest(sink)

    case Status.Failure(cause) =>
      log.error("Failure received", cause)
  }
}
// TODO [refactoring] merge this 'Durchlauferhitzer' with BitfinexTPWebSocketFlow