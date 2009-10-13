package org.s3q
import scala.xml._
import scala.xml.parsing._
import Environment._
import net.lag.configgy.Configgy
import net.lag.logging.Logger

case class S3Exception(val status: Int, val response:String) extends Exception {
  override def toString = {"error code " + status + ": " + response}
}

class S3Response(exchange: S3Exchange) {
  private val log = Logger.get

  lazy val whenFinished = {
    exchange.get
  }

  lazy val data = getData

  private def getData: Option[String] = {
    whenFinished match {
      case Right(e) => retry(e)
      case Left(exchange) => handleResponse(exchange)
    }
  }

  def handleResponse(exchange:S3Exchange): Option[String] = {
    if(exchange.status != 200){
      if(exchange.status == 404){
        log.debug("Received 404 response")
        return None
      }
      return retry(exchange)
    }
    log.debug("Received %s successful response", exchange.status)
    Some(exchange.getResponseContent)
  }

  def retry(error:Throwable): Option[String] = {
    if(!request.isRetriable){
      log.error("Received Throwable %s: Not Retrying", error)
      throw(error)
    } else {
      log.error("Received Throwable %s: Retrying", error)
      request.incrementAttempts
      return client.execute(request).data
    }
  }

  def retry(exchange:S3Exchange): Option[String] = {
    if(!request.isRetriable){
      log.error("Received %s error response: Not Retrying", exchange.status)
      throw(S3Exception(exchange.status, exchange.getResponseContent))
    } else {
      log.error("Received %s error response: Retrying", exchange.status)
      request.incrementAttempts
      return client.execute(request).data
    }
  }

  def request: S3Request = exchange.request

  def client: S3Client = exchange.client

  def verify = { }
}

class S3PutResponse(exchange: S3Exchange) extends S3Response(exchange: S3Exchange) {
  override def verify = {
    data
  }
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
