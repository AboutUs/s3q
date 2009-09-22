package org.s3q
import scala.xml._
import scala.xml.parsing._

class S3Exception extends Exception {}

class S3Response(exchange: S3Exchange) {
  lazy val whenFinished = {
    exchange.get
  }

  def data: Option[String] = {
    // Possibly we should not retry for other response types as well.
    if(status != 200){
      if(status == 404){
        return None
      }
      if(request.retries == 0){
        throw(new S3Exception)
      } else {
        request.retries -= 1
        return client.execute(request).data
      }
    }

    Some(whenFinished.getResponseContent)
  }

  def status: Int = {
    whenFinished.getResponseStatus
  }

  def request: S3Request = exchange.request

  def client: S3Client = exchange.client
}

class S3ListResponse(exchange: S3Exchange) extends S3Response(exchange: S3Exchange) {
  lazy val doc = { data match {
    case Some(string) => XML.loadString(string)
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