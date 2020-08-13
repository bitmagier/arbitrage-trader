package org.purevalue.arbitrage

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import org.purevalue.arbitrage.Utils.convertBytesToLowerCaseHex
import spray.json.{DeserializationException, JsValue, JsonParser, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

object HttpUtils {
  def query(uri: String)
           (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri
      )).flatMap(_.entity.toStrict(AppConfig.httpTimeout))
  }

  //[linux]$ echo -n "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559" | openssl dgst -sha256 -hmac "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
  //(stdin)= c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71
  def sha256Signature(entity: String, apiSecretKey: String): String = {
    import javax.crypto.Mac
    import javax.crypto.spec.SecretKeySpec

    val hasher = Mac.getInstance("HmacSHA256")
    hasher.init(new SecretKeySpec(apiSecretKey.getBytes, "HmacSHA256"))
    val hash: Array[Byte] = hasher.doFinal(entity.getBytes)
    convertBytesToLowerCaseHex(hash)
  }

  def queryBinanceHmaxSha256(uri: String, entity: String, apiKeys: SecretsConfig)
                            (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[HttpEntity.Strict] = {
    Http().singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        uri = uri,
        headers = List(headers.RawHeader("X-MBX-APIKEY", apiKeys.apiKey)),
        entity = HttpEntity(entity + s"&signature=${sha256Signature(entity, apiKeys.apiSecretKey)}")
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

  def queryPureJsonBinanceAccount(uri: String, entity: String, apiKeys: SecretsConfig)
                                 (implicit system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[JsValue] = {
    queryBinanceHmaxSha256(uri, entity, apiKeys).map { r =>
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

  def queryJsonBinanceAccount[T](uri: String, entity: String, apiKeys: SecretsConfig)
                                (implicit evidence: JsonReader[T], system: ActorSystem, fm: Materializer, executor: ExecutionContext): Future[T] = {
    queryPureJsonBinanceAccount(uri, entity, apiKeys).map { j =>
      try {
        j.convertTo[T]
      } catch {
        case e: DeserializationException =>
          throw new RuntimeException(s"Failed to parse response from $uri: $e\n$j")
      }
    }
  }
}