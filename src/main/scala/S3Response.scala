package org.s3q
import scala.xml._
import scala.xml.parsing._

class S3Exception extends Exception {}

class S3Response(exchange: S3Exchange) {
  lazy val whenFinished = {
    exchange.get
  }

  def data: String = {
    if(status != 200){
      if(request.retries == 0){
      throw(new S3Exception)
      } else {
        request.retries -= 1
        return client.execute(request).data
      }
    }

    whenFinished.getResponseContent
  }

  def status: Int = {
    whenFinished.getResponseStatus
  }

  def request: S3Request = exchange.request

  def client: S3Client = exchange.client
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