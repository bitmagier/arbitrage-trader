package org.purevalue.arbitrage.util

import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

object CryptoUtil {
  private def hmacSignature(algorithm: String, authPayload: Array[Byte], apiSecretKey: Array[Byte]): Array[Byte] = {
    val hasher = Mac.getInstance(algorithm)
    hasher.init(new SecretKeySpec(apiSecretKey, algorithm))
    hasher.doFinal(authPayload)
  }

  def hmacSha256Signature(authPayload: String, apiSecretKey: Array[Byte]): Array[Byte] =
    hmacSignature("HmacSHA256", authPayload.getBytes, apiSecretKey)

  def hmacSha384Signature(authPayload: String, apiSecretKey: Array[Byte]): Array[Byte] =
    hmacSignature("HmacSHA384", authPayload.getBytes, apiSecretKey)

  def hmacSha512Signature(authPayload: Array[Byte], apiSecretKey: Array[Byte]): Array[Byte] =
    hmacSignature("HmacSHA512", authPayload, apiSecretKey)

  def sha256Hash(input: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("SHA-256").digest(input)
  }
}
