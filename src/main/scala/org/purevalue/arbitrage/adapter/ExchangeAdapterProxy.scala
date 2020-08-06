package org.purevalue.arbitrage.adapter

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import org.purevalue.arbitrage.adapter.ExchangeAdapterProxy.{GetTradePairs, TradePairDataStreamRequest, TradePairs}
import org.purevalue.arbitrage.{ExchangeConfig, Main, TradePair}
import org.slf4j.LoggerFactory
import spray.json.{DeserializationException, JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContextExecutor, Future}

object ExchangeAdapterProxy {
  case class GetTradePairs()
  case class TradePairs(value: Set[TradePair])
  case class TradePairDataStreamRequest(tradePair: TradePair)
}

abstract class ExchangeAdapterProxy(config: ExchangeConfig) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[ExchangeAdapterProxy])
  implicit val actorSystem: ActorSystem = Main.actorSystem
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  def name: String

  def tradePairs: Set[TradePair]

  def startStreamingTradePairData(tradePair: TradePair, tradePairDataManager: ActorRef): Unit

  override def receive: Receive = {
    // Messages from Exchange

    case GetTradePairs =>
      sender() ! TradePairs(tradePairs)

    // Messages from TradePairDataManager

    case TradePairDataStreamRequest(tradePair) =>
      startStreamingTradePairData(tradePair, sender())

    case Status.Failure(cause) =>
      log.error("received failure", cause)
  }

  def query(uri: String): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      )).flatMap(_.entity.toStrict(config.httpTimeout))
  }

  def queryPureJson(uri: String): Future[JsValue] = {
    query(uri).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }
  }

  def queryJson[T](uri: String)(implicit evidence: JsonReader[T]): Future[T] = {
    queryPureJson(uri).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }
}
