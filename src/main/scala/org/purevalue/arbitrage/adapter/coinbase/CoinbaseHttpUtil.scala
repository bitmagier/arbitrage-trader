package org.purevalue.arbitrage.adapter.coinbase

import java.time.Instant
import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import org.purevalue.arbitrage.util.HttpUtil.hmacSha256Signature
import org.purevalue.arbitrage.{GlobalConfig, Main, SecretsConfig}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

private[coinbase] object CoinbaseHttpUtil {
  private val log = LoggerFactory.getLogger("org.purevalue.arbitrage.adapter.coinbase.CoinbaseHttpUtil")
  private val globalConfig: GlobalConfig = Main.config().global

  // https://docs.pro.coinbase.com/#api-key-permissions
  def httpRequestCoinbaseHmacSha256(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                   (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpResponse] = {
    val timestamp: String = (Instant.now.toEpochMilli / 1000).toString
    val requestPath = Uri(uri).path
    val contentToSign = s"""$timestamp$method$requestPath${requestBody.getOrElse("")}"""
    val secretKey = Base64.getDecoder.decode(apiKeys.apiSecretKey)
    val signature = Base64.getEncoder.encodeToString(hmacSha256Signature(contentToSign, secretKey))

    Http().singleRequest(
      HttpRequest(
        method,
        uri = Uri(uri),
        headers = List(
          RawHeader("CB-ACCESS_KEY", apiKeys.apiKey),
          RawHeader("CB-ACCESS-SIGN", signature),
          RawHeader("CB_ACCESS-TIMESTAMP", timestamp),
          RawHeader("CB-ACCESS-PASSPHRASE", apiKeys.apiKeyPassphrase.get)
        ),
        entity = requestBody match {
          case None => HttpEntity.Empty
          case Some(x) => HttpEntity(ContentTypes.`application/json`, x)
        }
      ))
  }

  def httpRequestPureJsonCoinbaseAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                        (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[(StatusCode, JsValue)] = {
    httpRequestCoinbaseHmacSha256(method, uri, requestBody, apiKeys)
      .flatMap {
        response: HttpResponse =>
          response.entity.toStrict(globalConfig.httpTimeout).map { r =>
            if (!response.status.isSuccess()) log.warn(s"$response")
            r.contentType match {
              case ContentTypes.`application/json` => (response.status, JsonParser(r.data.utf8String))
              case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
            }
          }
      }
  }

  def httpRequestJsonCoinbaseAccount[T, E](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                          (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[Either[T, E]] = {
    httpRequestPureJsonCoinbaseAccount(method, uri, requestBody, apiKeys).map {
      case (statusCode, j) =>
        try {
          if (statusCode.isSuccess()) Left(j.convertTo[T])
          else Right(j.convertTo[E])
        } catch {
          case e: Exception => throw new RuntimeException(s"$uri failed. Response: $statusCode, $j, ", e)
        }
    }
  }
}
