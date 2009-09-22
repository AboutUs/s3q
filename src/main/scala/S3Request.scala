package org.s3q

import Environment._

import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac

import org.apache.commons.codec.binary.Base64

import org.mortbay.jetty.client.ContentExchange
import org.mortbay.io.ByteArrayBuffer

abstract class S3Request {
  val MAX_TRIES = 3
  val client:S3Client
  var retries:Int = MAX_TRIES

  def verb: String
  def contentMd5: String
  def contentType: String
  def canonicalizedResource: String
  def url: String
  def body: Option[ByteArrayBuffer]

  def headers = {
    Map("Date" -> date, "Authorization" -> authorization)
  }


  def signature: String = {
    return calculateHMAC(stringToSign, client.config.secretAccessKey)
  }

  def authorization: String = {
    "AWS " + client.config.accessKeyId + ":" + signature
  }

  private def canonicalizedAmzHeaders = ""

  def stringToSign: String = {
    List(verb, contentMd5, contentType, date).foldLeft("")(_ + _ + "\n") +
      canonicalizedAmzHeaders + canonicalizedResource
  }

  val ALGORITHM = "HmacSHA1"
  private def calculateHMAC(data: String, key: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes("UTF-8"), ALGORITHM)
    val mac = Mac.getInstance(ALGORITHM)
    mac.init(signingKey)

    val rawHmac = mac.doFinal(data.getBytes())
    new String(Base64.encodeBase64(rawHmac))
  }

  lazy val date: String = {
    val format = new java.text.SimpleDateFormat(
        "EEE, dd MMM yyyy HH:mm:ss z", java.util.Locale.US)
    format.setTimeZone(new java.util.SimpleTimeZone(0, "GMT"))
    format.format(env.currentDate)
  }

  def response(exchange: S3Exchange) = { new S3Response(exchange) }

  def host = { client.config.hostname }

}

abstract class S3AbstractGet extends S3Request {
  override def verb = "GET"
  override def contentMd5 = ""
  override def contentType = ""
  override def body = None

  def encode(string: String):String = {
    java.net.URLEncoder.encode(string, "UTF-8")
  }

}

class S3Get(val client: S3Client, bucket: String, path: String) extends S3AbstractGet {
  override def canonicalizedResource = {
    "/" + bucket + "/" + path
  }
  override def url = { "http://" + host + "/" + bucket + "/" + path }
}

class S3List(val client: S3Client, bucket: String, items: Int, marker: Option[String]) extends S3AbstractGet {
  def this(client:S3Client, bucket: String, items: Int) = { this(client, bucket, items, None) }
  def this(client:S3Client, bucket: String, items: Int, marker: String) = { this(client, bucket, items, Some(marker)) }

  override def url = { "http://" + host + "/" + bucket + "/" + "?" + stringArgs }

  def stringArgs = {
    args.map {case (key, value) => key + "=" + encode(value) }.mkString("&")
  }

  def args = {
    Map("max_keys" -> items.toString) ++ (
      marker match {
        case Some(string) => Map("marker" -> string)
        case None => Map()
      })
  }

  override def canonicalizedResource = { "/" + bucket + "/" }

  override def response(exchange: S3Exchange) = { new S3ListResponse(exchange) }
}


class S3Put(val client: S3Client, bucket: String, path: String, data: String) extends S3Request {
  override def verb = "PUT"
  override def contentType = ""
  override def headers = {
    super.headers + ("Content-Md5" -> contentMd5)
  }

  override def canonicalizedResource = {
    "/" + bucket + "/" + path
  }
  override def body = { Some(new ByteArrayBuffer(data)) }
  override def host = { client.config.hostname }
  override def url = { "http://" + host + "/" + bucket + "/" + path }

  override def contentMd5 = {
    new String(Base64.encodeBase64(org.apache.commons.codec.digest.DigestUtils.md5(data)))
  }
}

