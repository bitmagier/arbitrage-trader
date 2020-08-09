package org.purevalue.arbitrage

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.Materializer
import spray.json.{DeserializationException, JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

object Utils {
  def query(uri: String, timeout:FiniteDuration)
           (implicit system:ActorSystem, fm:Materializer, executor:ExecutionContext): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      )).flatMap(_.entity.toStrict(timeout))
  }

  def queryPureJson(uri: String, timeout:FiniteDuration)
                   (implicit system:ActorSystem, fm:Materializer, executor:ExecutionContext): Future[JsValue] = {
    query(uri, timeout).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }
  }

  def queryJson[T](uri: String, timeout:FiniteDuration)
                  (implicit evidence: JsonReader[T], system:ActorSystem, fm:Materializer, executor:ExecutionContext): Future[T] = {
    queryPureJson(uri, timeout).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }
}
