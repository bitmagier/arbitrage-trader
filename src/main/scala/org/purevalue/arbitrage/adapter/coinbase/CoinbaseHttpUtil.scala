package org.purevalue.arbitrage.adapter.coinbase

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import org.purevalue.arbitrage.util.CryptoUtil.hmacSha256Signature
import org.purevalue.arbitrage.{GlobalConfig, Main, SecretsConfig}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}

private[coinbase] object CoinbaseHttpUtil {
  private val log = LoggerFactory.getLogger(getClass)
  private lazy val globalConfig: GlobalConfig = Main.config().global

  // {"iso":"2020-10-01T21:22:24Z","epoch":1601587344.} <- spray cannot parse that, but we can
  def parseServerTime(jsonLike: String): Double = {
    val end = jsonLike.lastIndexOf('}')
    val start = jsonLike.lastIndexOf(':') + 1
    jsonLike.substring(start, end).toDouble
  }

  case class Signature(cbAccessKey: String, cbAccessSign: String, cbAccessTimestamp: String, cbAccessPassphrase: String)

  def createSignature(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, serverTime: Double): Signature = {
    // [coinbase documentation]
    // The CB-ACCESS-TIMESTAMP header MUST be number of seconds since Unix Epoch in UTC. Decimal values are allowed
    // Your timestamp must be within 30 seconds of the api service time or your request will be considered expired and rejected.
    // We recommend using the time endpoint to query for the API server time if you believe there many be time skew between your server and the API servers.
    val timestamp: String = serverTime.toString
    val requestPath = Uri(uri).toRelative.toString()
    val contentToSign = s"""$timestamp${method.value}$requestPath${requestBody.getOrElse("")}"""
    val secretKey = Base64.getDecoder.decode(apiKeys.apiSecretKey)
    val signature = new String(Base64.getEncoder.encode(hmacSha256Signature(contentToSign, secretKey)), StandardCharsets.ISO_8859_1)
    Signature(apiKeys.apiKey, signature, timestamp, apiKeys.apiKeyPassphrase.get)
  }

  // https://docs.pro.coinbase.com/#api-key-permissions
  def httpRequestCoinbaseHmacSha256(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, serverTime: Double)
                                   (implicit system: ActorSystem[_], fm: Materializer, executor: ExecutionContext):
  Future[HttpResponse] = {
    val signature = createSignature(method, uri, requestBody, apiKeys, serverTime)

    Http().singleRequest(
      HttpRequest(
        method,
        uri = Uri(uri),
        headers = List(
          RawHeader("CB-ACCESS-KEY", signature.cbAccessKey),
          RawHeader("CB-ACCESS-SIGN", signature.cbAccessSign),
          RawHeader("CB-ACCESS-TIMESTAMP", signature.cbAccessTimestamp),
          RawHeader("CB-ACCESS-PASSPHRASE", signature.cbAccessPassphrase)
        ),
        entity = requestBody match {
          case None => HttpEntity.Empty
          case Some(x) => HttpEntity(ContentTypes.`application/json`, x)
        }
      ))
  }

  def httpRequestCoinbaseAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, serverTime: Double)
                                (implicit system: ActorSystem[_], fm: Materializer, executor: ExecutionContext):
  Future[(StatusCode, String)] = {
    httpRequestCoinbaseHmacSha256(method, uri, requestBody, apiKeys, serverTime)
      .flatMap {
        response: HttpResponse =>
          response.entity.toStrict(globalConfig.httpTimeout).map { r =>
            if (!response.status.isSuccess()) log.warn(s"$response")
            (response.status, r.data.utf8String)
          }
      }
  }

  def httpRequestPureJsonCoinbaseAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, serverTime: Double)
                                        (implicit system: ActorSystem[_], fm: Materializer, executor: ExecutionContext):
  Future[(StatusCode, JsValue)] = {
    httpRequestCoinbaseHmacSha256(method, uri, requestBody, apiKeys, serverTime)
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

  def httpRequestJsonCoinbaseAccount[T, E](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, serverTime: Double)
                                          (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem[_], fm: Materializer,
                                           executor: ExecutionContext):
  Future[Either[T, E]] = {
    httpRequestPureJsonCoinbaseAccount(method, uri, requestBody, apiKeys, serverTime).map {
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
