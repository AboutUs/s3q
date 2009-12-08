package org.s3q

import scala.xml._
import scala.xml.parsing._
import Environment._

import org.xlightweb.IHttpResponse

case class S3Exception(val status: Int, val response:String) extends Exception {
  override def toString = {"error code " + status + ": " + response}
}

case class BadResponseCode(val code: Int, val response: String) extends Exception {
  def this(code: Int) = this(code, "")
  override def toString = "BadResponseCode:" + code + "\nResponse:\n" + response
}



class S3ResponseFuture(val request: S3Request, val client:S3Client, future: Types.ResponseFuture) {
  private val log = Environment.env.logger

  lazy val response = future() match {
    case Right(responseOrFailure) => responseOrFailure
    case Left(timeout) => Left(timeout)
  }

}

class S3Response(response: HttpResponse) {

  lazy val data:Option[Array[Byte]] = {
    response.status match {
      case 404 => None
      case _ => Some(response.body)
    }
  }

  lazy val dataString = data.map(new String(_))

  def status = response.status
  def headers = response.headers

  def header(key: String) = headers.get(key.toLowerCase)

}

class S3PutResponse(response: HttpResponse) extends S3Response(response: HttpResponse) {
}

class S3ListResponse(response: HttpResponse) extends S3Response(response: HttpResponse) {
  lazy val doc = data.map((d) => XML.loadString(new String(d, "UTF-8")))

  lazy val items: Seq[String] = {
    (doc.get \\ "Contents" \\ "Key").map { _.text }
  }

  lazy val isTruncated = {
    (doc.get \\ "IsTruncated").text == "true"
  }

}
