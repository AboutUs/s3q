package org.s3q
import scala.xml._
import scala.xml.parsing._
import Environment._

import org.xlightweb.IHttpResponse

case class S3Exception(val status: Int, val response:String) extends Exception {
  override def toString = {"error code " + status + ": " + response}
}

case class BadResponseCode(code: Int) extends Exception {
  override def toString = "BadResponseCode:" + code
}

class S3ResponseFuture(handler: S3RequestHandler) {
  private val log = Environment.env.logger

  lazy val response:Either[Throwable, S3Response] = {
    handler.whenFinished match {
      case Right(response) => response.isOk match {
        case true => Right(handler.request.response(response))
        case false => Left(BadResponseCode(response.status))
      }
      case Left(ex) => Left(ex)
    }
  }

  def retry(error:Throwable) = {
    request.isRetriable match {
      case false => {
        log.error("Received Throwable %s: Not Retrying", error)
        Left(error)
      }
      case true => {
        log.error("Received Throwable %s: Retrying", error)
        request.incrementAttempts
        val r = client.execute(request).response

        r
      }
    }
  }

  val request = handler.request

  val client = handler.client

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

  def verify = { }

}

class S3PutResponse(response: HttpResponse) extends S3Response(response: HttpResponse) {
  override def verify = {
    data
  }
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
