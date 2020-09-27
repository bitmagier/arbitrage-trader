package org.purevalue.arbitrage.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import org.purevalue.arbitrage.{GlobalConfig, Main}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object HttpUtil {
  private val log = LoggerFactory.getLogger(HttpUtil.getClass)
  private val globalConfig: GlobalConfig = Main.config().global

  def query(uri: String)
           (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      ))
  }


  def hmacSha256Signature(authPayload: String, apiSecretKey: Array[Byte]): Array[Byte] = {
    import javax.crypto.Mac
    import javax.crypto.spec.SecretKeySpec

    val hasher = Mac.getInstance("HmacSHA256")
    hasher.init(new SecretKeySpec(apiSecretKey, "HmacSHA256"))
    hasher.doFinal(authPayload.getBytes)
  }

  def hmacSha384Signature(authPayload: String, apiSecretKey: Array[Byte]): Array[Byte] = {
    import javax.crypto.Mac
    import javax.crypto.spec.SecretKeySpec

    val hasher = Mac.getInstance("HmacSHA384")
    hasher.init(new SecretKeySpec(apiSecretKey, "HmacSHA384"))
    hasher.doFinal(authPayload.getBytes)
  }


  def httpGetPureJson(uri: String)
                     (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[(StatusCode, JsValue)] = {
    query(uri)
      .flatMap {
        response: HttpResponse =>
          if (!response.status.isSuccess()) log.warn(s"$response")
          response.entity.toStrict(globalConfig.httpTimeout)
            .map { r =>
              r.contentType match {
                case ContentTypes.`application/json` => (response.status, JsonParser(r.data.utf8String))
                case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
              }
            }
      }
  }


  def httpGetJson[T, E](uri: String)
                       (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[Either[T, E]] = {
    httpGetPureJson(uri).map {
      case (statusCode, j) =>
        try {
          if (statusCode.isSuccess()) Left(j.convertTo[T])
          else Right(j.convertTo[E])
        } catch {
          case e: Exception => throw new RuntimeException(s"$uri failed", e)
        }
    }
  }
}