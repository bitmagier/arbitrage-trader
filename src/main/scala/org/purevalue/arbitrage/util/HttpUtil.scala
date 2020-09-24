package org.purevalue.arbitrage.util

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import org.purevalue.arbitrage.util.Util.convertBytesToLowerCaseHex
import org.purevalue.arbitrage.{GlobalConfig, Main, SecretsConfig}
import org.slf4j.LoggerFactory
import spray.json.{JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object HttpUtil {
  private val log = LoggerFactory.getLogger(HttpUtil.getClass)
  val globalConfig: GlobalConfig = Main.config().global

  def query(uri: String)
           (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      ))
  }

  //[linux]$ echo -n "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559" | openssl dgst -sha256 -hmac "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
  //(stdin)= c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71
  def hmacSha256Signature(authPayload: String, apiSecretKey: String): String = {
    import javax.crypto.Mac
    import javax.crypto.spec.SecretKeySpec

    val hasher = Mac.getInstance("HmacSHA256")
    hasher.init(new SecretKeySpec(apiSecretKey.getBytes, "HmacSHA256"))
    val hash: Array[Byte] = hasher.doFinal(authPayload.getBytes)
    convertBytesToLowerCaseHex(hash)
  }

  def hmacSha384Signature(authPayload: String, apiSecretKey: String): String = {
    import javax.crypto.Mac
    import javax.crypto.spec.SecretKeySpec

    val hasher = Mac.getInstance("HmacSHA384")
    hasher.init(new SecretKeySpec(apiSecretKey.getBytes, "HmacSHA384"))
    val hash: Array[Byte] = hasher.doFinal(authPayload.getBytes)
    convertBytesToLowerCaseHex(hash)
  }

  def httpRequestBinanceHmacSha256(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, sign: Boolean)
                                  (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpResponse] = {
    val finalUriParams: Option[String] =
      if (sign) {
        val totalParamsBeforeSigning: String = (Uri(uri).queryString() match {
          case None => ""
          case Some(x) => x + "&"
        }) + s"timestamp=${Instant.now.toEpochMilli}"
        val contentToSign = totalParamsBeforeSigning + requestBody.getOrElse("")
        Some(totalParamsBeforeSigning + "&" + s"signature=${hmacSha256Signature(contentToSign, apiKeys.apiSecretKey)}")
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

  var lastBitfinexNonce: Option[Long] = None

  def bitfinexNonce: String = synchronized {
    var nonce = Instant.now.toEpochMilli * 1000
    if (lastBitfinexNonce.isDefined && lastBitfinexNonce.get == nonce) {
      nonce = nonce + 1
    }
    lastBitfinexNonce = Some(nonce)
    nonce.toString
  }

  def httpRequestBitfinexHmacSha384(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                   (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpResponse] = {
    val nonce = bitfinexNonce
    val apiPath = Uri(uri).toRelative.toString() match {
      case s: String if s.startsWith("/") => s.substring(1) // "v2/auth/..."
      case _ => throw new WrongAssumption("relative url starts with /")
    }
    val contentToSign = s"/api/$apiPath$nonce${requestBody.getOrElse("")}"
    val signature = hmacSha384Signature(contentToSign, apiKeys.apiSecretKey)

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

  def httpRequestPureJsonBitfinexAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                        (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[(StatusCode, JsValue)] =
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

  def httpRequestJsonBitfinexAccount[T, E](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                          (implicit evidence1: JsonReader[T], evidence2: JsonReader[E], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[Either[T, E]] = {
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