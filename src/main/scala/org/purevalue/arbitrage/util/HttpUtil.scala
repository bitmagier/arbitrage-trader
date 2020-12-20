package org.purevalue.arbitrage.util

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import org.purevalue.arbitrage.{GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object HttpUtil {
  private val log = LoggerFactory.getLogger(getClass)
  private val globalConfig: GlobalConfig = Main.config().global

  def httpGet(uri: String)
             (implicit system: ActorSystem[_]): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(method = HttpMethods.GET, uri = uri)
    )
  }

  private def readJsonResponse(response: HttpResponse)
                              (implicit system: ActorSystem[_], executor: ExecutionContext): Future[(StatusCode, JsValue)] = {
    if (!response.status.isSuccess()) log.warn(s"$response")
    response.entity.toStrict(globalConfig.httpTimeout)
      .map { r =>
        r.contentType match {
          case ContentTypes.`application/json` => (response.status, JsonParser(r.data.utf8String))
          case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
        }
      }
  }

  //  def httpGetPureJson(uri: String)
  //                     (implicit system: ActorSystem[_], executor: ExecutionContext): Future[(StatusCode, JsValue)] = {
  //    httpGet(uri)
  //      .flatMap(readJsonResponse)
  //  }

  //  def httpGetJson[T, E](uri: String)
  //                       (implicit evidence1: JsonReader[T],
  //                        evidence2: JsonReader[E],
  //                        system: ActorSystem[_],
  //                        executor: ExecutionContext): Future[Either[T, E]] = {
  //    httpGetPureJson(uri)
  //      .map {
  //        case (statusCode, j) =>
  //          try {
  //            if (statusCode.isSuccess()) Left(j.convertTo[T])
  //            else Right(j.convertTo[E])
  //          } catch {
  //            case e: Exception => throw new RuntimeException(s"GET $uri failed", e)
  //          }
  //      }
  //  }
  def httpGetJson[T, E](uri: String)
                       (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem[_], executor: ExecutionContext): Future[Either[T, E]] = {
    httpJson[T, E](HttpMethods.GET, uri, HttpEntity.Empty)
  }

  def http(method: HttpMethod, uri: String, entity: RequestEntity)
          (implicit system: ActorSystem[_]): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(
        method = method,
        uri = uri,
        entity = entity
      )
    )
  }

  def httpPureJson(method: HttpMethod, uri: String, entity: RequestEntity)
                  (implicit system: ActorSystem[_], executor: ExecutionContext):
  Future[(StatusCode, JsValue)] = {
    http(method, uri, entity)
      .flatMap(readJsonResponse)
  }

  def httpJson[T, E](method: HttpMethod, uri: String, entity: RequestEntity)
                    (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem[_], executor: ExecutionContext): Future[Either[T, E]] = {
    httpPureJson(method, uri, entity)
      .map {
        case (statusCode, j) =>
          try {
            if (statusCode.isSuccess()) Left(j.convertTo[T])
            else Right(j.convertTo[E])
          } catch {
            case e: Exception => throw new RuntimeException(s"${method.value} $uri failed", e)
          }
      }
  }
}