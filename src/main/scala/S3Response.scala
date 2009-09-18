package org.s3q
import scala.xml._
import scala.xml.parsing._

class S3Response(exchange: S3Exchange) {
  lazy val whenFinished = {
    exchange.get
  }

  def data: String = {
    whenFinished.getResponseContent
  }

  def status: Int = {
    whenFinished.getResponseStatus
  }

}

class S3ListResponse(exchange: S3Exchange) extends S3Response(exchange: S3Exchange) {
  lazy val doc = { XML.loadString(data) }

  def items: Seq[String] = {
    (doc \\ "Contents" \\ "Key").map { _.text }
  }

  def isTruncated = {
    (doc \\ "IsTruncated").text == "true"
  }

}