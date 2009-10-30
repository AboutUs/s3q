package org.s3q
import scala.xml._
import scala.xml.parsing._
import Environment._

import scala.collection.jcl.Conversions._


case class S3Exception(val status: Int, val response:String) extends Exception {
  override def toString = {"error code " + status + ": " + response}
}

class S3Response(handler: S3RequestHandler) {
  private val log = Environment.env.logger

  lazy val whenFinished = {
    handler.whenFinished
  }

  lazy val data = {
    status match {
      case 404 => None
      case _ => Some(whenFinished.getBlockingBody.readBytes)
    }
  }

  def status = whenFinished.getStatus

  lazy val headers = {
    val resp = whenFinished
    resp.getHeaderNameSet.
      foldLeft(Map[String, String]()) {(m, key) => m(key.toLowerCase) = resp.getHeader(key) }
  }

  def header(key: String) = headers.get(key.toLowerCase)

  def request: S3Request = handler.request

  def client: S3Client = handler.client

  def verify = { }
}

class S3PutResponse(handler: S3RequestHandler) extends S3Response(handler: S3RequestHandler) {
  override def verify = {
    data
  }
}

class S3ListResponse(handler: S3RequestHandler) extends S3Response(handler: S3RequestHandler) {
  lazy val doc = { data match {
    case Some(bytes) => XML.loadString(new String(bytes, "UTF-8"))
    case None => null
    }
  }

  def items: Seq[String] = {
    (doc \\ "Contents" \\ "Key").map { _.text }
  }

  def isTruncated = {
    (doc \\ "IsTruncated").text == "true"
  }

}
