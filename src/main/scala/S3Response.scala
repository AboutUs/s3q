package org.s3q
import scala.xml._
import scala.xml.parsing._
import Environment._

import scala.collection.jcl.Conversions._

import org.xlightweb.IHttpResponse

case class S3Exception(val status: Int, val response:String) extends Exception {
  override def toString = {"error code " + status + ": " + response}
}

class S3ResponseFuture(handler: S3RequestHandler) {
  private val log = Environment.env.logger

  case class BadResponseCode(code: Int) extends Exception

  lazy val response:Either[Throwable, S3Response] = {
    handler.whenFinished match {
      case Right(response) => response.getStatus match {
        case s if 200 to 299 contains s => Right(new S3Response(response))
        case 404 => Right(new S3Response(response))
        case s => retry(BadResponseCode(s))
      }
      case Left(ex) => retry(ex)
    }
  }

  def retry(error:Throwable) = {
    throw(error)
//    request.isRetriable match {
//     case false => {
//       log.error("Received Throwable %s: Not Retrying", error)
//       Left(error)
//     }
//     case true => {
//       log.error("Received Throwable %s: Retrying", error)
//       request.incrementAttempts
//       client.execute(request).response
//     }
//   }
  }

  val request = handler.request

  val client = handler.client

}

class S3Response(response: IHttpResponse) {

  lazy val data:Option[Array[Byte]] = {
    status match {
      case 404 => None
      case _ => Some(response.getBlockingBody.readBytes)
    }
  }

  lazy val dataString = data.map(new String(_))

  def status = response.getStatus

  lazy val headers = {
    response.getHeaderNameSet.
      foldLeft(Map[String, String]()) {(m, key) => m + (key.toLowerCase -> response.getHeader(key)) }
  }

  def header(key: String) = headers.get(key.toLowerCase)

  def verify = { }

}

class S3PutResponse(response: IHttpResponse) extends S3Response(response: IHttpResponse) {
  override def verify = {
    data
  }
}

class S3ListResponse(response: IHttpResponse) extends S3Response(response: IHttpResponse) {
  lazy val doc = data.map((d: Array[Byte]) => XML.loadString(new String(d, "UTF-8")))

  lazy val items: Seq[String] = {
    (doc.get \\ "Contents" \\ "Key").map { _.text }
  }

  lazy val isTruncated = {
    (doc.get \\ "IsTruncated").text == "true"
  }

}
