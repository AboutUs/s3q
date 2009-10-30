package org.s3q
import scala.xml._
import scala.xml.parsing._
import Environment._


case class S3Exception(val status: Int, val response:String) extends Exception {
  override def toString = {"error code " + status + ": " + response}
}

class S3Response(exchange: S3Exchange) {
  private val log = Environment.env.logger

  lazy val whenFinished = {
    exchange.get
  }

  lazy val data = getData

  private def getData: Option[Array[Byte]] = {
    whenFinished match {
      case Left(e) => retry(e)
      case Right(exchange) => handleResponse(exchange)
    }
  }

  def handleResponse(exchange:S3Exchange): Option[Array[Byte]] = {
    if(!(200 to 299).contains(exchange.status)){
      if(exchange.status == 404){
        log.debug("Received 404 response")
        return None
      }
      return retry(exchange)
    }
    log.debug("Received %s successful response", exchange.status)
    Some(exchange.getResponseBytes)
  }

  def retry(error:Throwable): Option[Array[Byte]] = {
    if(!request.isRetriable){
      log.error("Received Throwable %s: Not Retrying", error)
      throw(error)
    } else {
      log.error("Received Throwable %s: Retrying", error)
      request.incrementAttempts
      while(client.queueFull) { Thread.sleep(100) }
      client.execute(request) match {
        case Some(response) => response.data
        case None => None
      }
    }
  }

  def retry(exchange:S3Exchange): Option[Array[Byte]] = {
    if(!request.isRetriable){
      log.error("Received %s error response: Not Retrying", exchange.status)
      throw(S3Exception(exchange.status, exchange.getResponseContent))
    } else {
      log.error("Received %s error response: Retrying\nWith message:", exchange.status, exchange.getResponseContent)
      request.incrementAttempts
      client.execute(request) match {
        case Some(response) => response.data
        case None => None
      }
    }
  }

  lazy val headers: scala.collection.Map[String, String] = whenFinished.right.get.responseHeaders

  def header(key: String) = headers.get(key.toLowerCase)

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
