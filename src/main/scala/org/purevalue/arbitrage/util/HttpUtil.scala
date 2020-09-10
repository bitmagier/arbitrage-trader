package org.purevalue.arbitrage.util

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import org.purevalue.arbitrage.util.Util.convertBytesToLowerCaseHex
import org.purevalue.arbitrage.{Config, SecretsConfig}
import spray.json.{DeserializationException, JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object HttpUtil {

  def query(uri: String)
           (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      )).flatMap(_.entity.toStrict(Config.httpTimeout))
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
                                  (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
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
      )).flatMap(_.entity.toStrict(Config.httpTimeout))
  }

  def httpRequestBitfinexHmacSha384(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                   (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
    val nonce = Instant.now.toEpochMilli.toString
    val apiPath = Uri(uri).withScheme("").withHost("") // "/v2/auth/..."
    val contentToSign = s"/api$apiPath$nonce${requestBody.getOrElse("")}}"
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
          case Some(x) => HttpEntity(x)
        }
      )).flatMap(_.entity.toStrict(Config.httpTimeout))
  }

  def httpGetPureJson(uri: String)
                     (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[JsValue] = {
    query(uri).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }
  }

  def httpRequestPureJsonBinanceAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, signed: Boolean)
                                       (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[JsValue] =
    httpRequestBinanceHmacSha256(method, uri, requestBody, apiKeys, signed).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }

  def httpRequestPureJsonBitfinexAccount(method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                        (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[JsValue] =
    httpRequestBitfinexHmacSha384(method, uri, requestBody, apiKeys).map { r =>
      r.contentType match {
        case ContentTypes.`application/json` =>
          JsonParser(r.data.utf8String)
        case _ => throw new RuntimeException(s"Non-Json message received:\n${r.data.utf8String}")
      }
    }

  def httpGetJson[T](uri: String)
                    (implicit evidence: JsonReader[T], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[T] = {
    httpGetPureJson(uri).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }

  def httpRequestJsonBinanceAccount[T](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig, sign: Boolean)
                                      (implicit evidence: JsonReader[T], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[T] = {
    httpRequestPureJsonBinanceAccount(method, uri, requestBody, apiKeys, sign).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }

  def httpRequestJsonBitfinexAccount[T](method: HttpMethod, uri: String, requestBody: Option[String], apiKeys: SecretsConfig)
                                       (implicit evidence: JsonReader[T], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[T] = {
    httpRequestPureJsonBitfinexAccount(method, uri, requestBody, apiKeys).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }
}