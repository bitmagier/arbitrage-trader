package org.purevalue.arbitrage.adapter.binance

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import org.purevalue.arbitrage.util.HttpUtil.hmacSha256Signature
import org.purevalue.arbitrage.util.Util.convertBytesToLowerCaseHex
import org.purevalue.arbitrage.{GlobalConfig, Main, SecretsConfig}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object BinanceHttpUtil {
  private val log = LoggerFactory.getLogger(BinanceHttpUtil.getClass)
  private val globalConfig: GlobalConfig = Main.config().global

  def httpRequestBinanceHmacSha256(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, sign: Boolean)
                                  (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpResponse] = {
    val finalUriParams: Option[String] =
      if (sign) {
        val totalParamsBeforeSigning: String = (Uri(uri).queryString() match {
          case None => ""
          case Some(x) => x + "&"
        }) + s"timestamp=${Instant.now.toEpochMilli}"
        val contentToSign = totalParamsBeforeSigning + requestBody.getOrElse("")
        val signature = convertBytesToLowerCaseHex(hmacSha256Signature(contentToSign, apiKeys.apiSecretKey.getBytes))
        Some(totalParamsBeforeSigning + "&" + s"signature=$signature")
      } else {
        Uri(uri).rawQueryString
      }

    Http().singleRequest(
      HttpRequest(
        method,
        uri = Uri(uri).withRawQueryString(finalUriParams.getOrElse("")),
        headers = List(RawHeader("X-MBX-APIKEY", apiKeys.apiKey)),
        entity = requestBody match {
          case None => HttpEntity.Empty
          case Some(x) => HttpEntity(x)
        }
      ))
  }


  def httpRequestPureJsonBinanceAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, sign: Boolean)
                                       (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[(StatusCode, JsValue)] = {
    httpRequestBinanceHmacSha256(method, uri, requestBody, apiKeys, sign)
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

  def httpRequestJsonBinanceAccount[T, E](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, sign: Boolean)
                                         (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[Either[T, E]] = {
    httpRequestPureJsonBinanceAccount(method, uri, requestBody, apiKeys, sign).map {
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
