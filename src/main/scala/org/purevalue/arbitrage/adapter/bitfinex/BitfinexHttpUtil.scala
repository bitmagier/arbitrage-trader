package org.purevalue.arbitrage.adapter.bitfinex

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import org.purevalue.arbitrage.util.CryptoUtil.hmacSha384Signature
import org.purevalue.arbitrage.util.Util.convertBytesToLowerCaseHex
import org.purevalue.arbitrage.util.WrongAssumption
import org.purevalue.arbitrage.{GlobalConfig, Main, SecretsConfig}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

private[bitfinex] object BitfinexHttpUtil {
  private val log = LoggerFactory.getLogger(getClass)
  private val globalConfig: GlobalConfig = Main.config().global
  private var lastBitfinexNonce: Option[Long] = None

  def bitfinexNonce: String = synchronized {
    var nonce = Instant.now.toEpochMilli * 1000
    if (lastBitfinexNonce.isDefined && lastBitfinexNonce.get == nonce) {
      nonce = nonce + 1
    }
    lastBitfinexNonce = Some(nonce)
    nonce.toString
  }

  def httpRequestBitfinexHmacSha384(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                   (implicit system: ActorSystem[_], fm: Materializer, executor: ExecutionContext):
  Future[HttpResponse] = {
    val nonce = bitfinexNonce
    val apiPath = Uri(uri).toRelative.toString() match {
      case s: String if s.startsWith("/") => s.substring(1) // "v2/auth/..."
      case _ => throw new WrongAssumption("relative url starts with /")
    }
    val contentToSign = s"/api/$apiPath$nonce${requestBody.getOrElse("")}"
    val signature = convertBytesToLowerCaseHex(hmacSha384Signature(contentToSign, apiKeys.apiSecretKey.getBytes))

    Http().singleRequest(
      HttpRequest(
        method,
        uri = Uri(uri),
        headers = List(
          RawHeader("bfx-nonce", nonce),
          RawHeader("bfx-apikey", apiKeys.apiKey),
          RawHeader("bfx-signature", signature)
        ),
        entity = requestBody match {
          case None => HttpEntity.Empty
          case Some(x) => HttpEntity(ContentTypes.`application/json`, x)
        }
      ))
  }

  def httpRequestPureJsonBitfinexAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                        (implicit system: ActorSystem[_], fm: Materializer, executor: ExecutionContext):
  Future[(StatusCode, JsValue)] =
    httpRequestBitfinexHmacSha384(method, uri, requestBody, apiKeys)
      .flatMap {
        response: HttpResponse =>
          if (!response.status.isSuccess()) log.warn(s"$response")
          response.entity.toStrict(globalConfig.httpTimeout).map { r =>
            r.contentType match {
              case ContentTypes.`application/json` => (response.status, JsonParser(r.data.utf8String))
              case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
            }
          }
      }

  def httpRequestJsonBitfinexAccount[T, E](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                          (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem[_],
                                           fm: Materializer, executor: ExecutionContext):
  Future[Either[T, E]] = {
    httpRequestPureJsonBitfinexAccount(method, uri, requestBody, apiKeys).map {
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
