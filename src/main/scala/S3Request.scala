package org.s3q

import Environment._

import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac

import org.apache.commons.codec.binary.Base64

import org.mortbay.jetty.client.ContentExchange
import org.mortbay.io.ByteArrayBuffer

abstract class S3Request {
  val MAX_ATTEMPTS = 3
  val client:S3Client
  val bucket: String
  val path: String = ""
  var attempts:Int = 1

  def verb: String
  def contentMd5: String = ""
  def contentType: String = ""
  def body: Option[ByteArrayBuffer] = None

  def url = "http://" + host + canonicalizedResource

  def canonicalizedResource = "/" + bucket + "/" + path

  def host = client.config.hostname

  def headers = Map("Date" -> date, "Authorization" -> authorization)

  def signature: String =
    return calculateHMAC(stringToSign, client.config.secretAccessKey)

  def authorization: String =
    "AWS " + client.config.accessKeyId + ":" + signature

  protected def canonicalizedAmzHeaders = ""

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
    format.format(Environment.env.currentDate)
  }

  def response(exchange: S3Exchange) = new S3Response(exchange)

  def incrementAttempts = synchronized {
    Environment.env.sleep((500 * java.lang.Math.pow(2,attempts)).toLong)
    attempts += 1
  }

  def isRetriable: Boolean =
    synchronized { attempts <= MAX_ATTEMPTS }

  def callback(response: Option[S3Response]) = { }

}

abstract class S3AbstractGet extends S3Request {
  override def verb = "GET"
  def encode(string:String):String = java.net.URLEncoder.encode(string, "UTF-8")
}

class S3Get(val client: S3Client, val bucket: String,
    override val path: String) extends S3AbstractGet

class S3List(val client: S3Client, val bucket: String, items: Int,
    marker: Option[String]) extends S3AbstractGet {

  def this(client:S3Client, bucket: String, items: Int)
    = this(client, bucket, items, None)

  def this(client:S3Client, bucket: String, items: Int, marker: String)
    = this(client, bucket, items, Some(marker))

  override def url = super.url + "?" + stringArgs

  def stringArgs =
    args.map {case (key, value) => key + "=" + encode(value) }.mkString("&")

  def args = {
    Map("max_keys" -> items.toString) ++ (
      marker match {
        case Some(string) => Map("marker" -> string)
        case None => Map()
      }
    )
  }

  override def response(exchange: S3Exchange) = new S3ListResponse(exchange)
}

class S3Delete(val client: S3Client, val bucket: String,
    override val path: String) extends S3Request {

  override def verb = "DELETE"
}


class S3Put(val client: S3Client, val bucket: String,
    override val path: String, data: Array[Byte], extraHeaders: Map[String, String]) extends S3Request {

  def this(client: S3Client, bucket: String, path: String, data: Array[Byte])
    = this(client, bucket, path, data, Map())

  override def verb = "PUT"
  override def headers = super.headers + ("Content-Md5" -> contentMd5) ++ extraHeaders

  override def body = Some(new ByteArrayBuffer(data))

  override def contentMd5 =
    new String(Base64.encodeBase64(org.apache.commons.codec.digest.DigestUtils.md5(data)))

  override def response(exchange: S3Exchange) = new S3PutResponse(exchange)

  // TODO: allow setting Content-Type
  override def canonicalizedAmzHeaders = extraHeaders.
    map { case (k,v) => k.toLowerCase -> v }.
    filter { case (k,v) => k.startsWith("x-amz-") }.toList.
    sort { case ((k1, v1), (k2, v2)) => k1 < k2 }.
    map { case (k,v) => k + ":" + v + "\n" }.mkString
}
