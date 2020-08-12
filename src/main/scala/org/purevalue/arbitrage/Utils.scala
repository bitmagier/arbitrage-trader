package org.purevalue.arbitrage

import java.text.DecimalFormat

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult.Ok
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import spray.json.{DeserializationException, JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object Utils {
  def query(uri: String)
           (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      )).flatMap(_.entity.toStrict(AppConfig.httpTimeout))
  }

  def sha256Signature(entity: String, apiKey: String): String = ???

  def queryBinanceHmaxSha256(uri: String, entity: String, binanceApiKey: String)
                            (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri,
        headers = List(headers.RawHeader("X-MBX-APIKEY", binanceApiKey)),
        entity = HttpEntity(entity + s"&signature=${sha256Signature(entity, binanceApiKey)}")
      )).flatMap(_.entity.toStrict(AppConfig.httpTimeout))
  }

  def queryPureJson(uri: String)
                   (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[JsValue] = {
    query(uri).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }
  }

  def queryPureJsonBinanceAccount(uri: String, entity: String, binanceApiKey: String)
                                 (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[JsValue] = {
    queryBinanceHmaxSha256(uri, entity, binanceApiKey).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }
  }

  def queryJson[T](uri: String)
                  (implicit evidence: JsonReader[T], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[T] = {
    queryPureJson(uri).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }

  def queryJsonBinanceAccount[T](uri: String, entity: String, binanceApiKey: String)
                                (implicit evidence: JsonReader[T], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[T] = {
    queryPureJsonBinanceAccount(uri, entity, binanceApiKey).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }

  def formatDecimal(d: Double): String = new DecimalFormat("#.##########").format(d)
}
