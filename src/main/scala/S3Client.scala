package org.s3q

import com.aboutus.auctors.kernel.reactor.{DefaultCompletableFutureResult, FutureTimeoutException}

import org.mortbay.jetty.client.ContentExchange
import org.mortbay.io.Buffer

import java.util.concurrent._


class S3Client(val secretAccessKey: String, val accessKeyId: String, maxConcurrency: Int) {
  def this(secretAccessKey: String, accessKeyId: String) = this(secretAccessKey, accessKeyId, 500)

  val activeRequests = new ArrayBlockingQueue[S3Request](maxConcurrency)

  val client = new org.mortbay.jetty.client.HttpClient
  client.setConnectorType(org.mortbay.jetty.client.HttpClient.CONNECTOR_SELECT_CHANNEL)

  client.start

  def execute(request: S3Request): S3Response = {
    val exchange = new S3Exchange(request, activeRequests)

    activeRequests.put(request)
    client.send(exchange)

    request.response(exchange)
  }

  def execute(request: S3List): S3ListResponse = {
    execute(request.asInstanceOf[S3Request]).asInstanceOf[S3ListResponse]
  }

}

class S3Exchange(request: S3Request, activeRequests: BlockingQueue[S3Request]) extends ContentExchange {
  setMethod(request.verb)
  setURL(request.url)

  for ((key, value) <- request.headers) {
    setRequestHeader(key, value)
  }

  val future = new DefaultCompletableFutureResult(60 * 1000)

  def get: S3Exchange = {
    try {
      future.await
    }
    catch {
      case e: FutureTimeoutException => throw new TimeoutException
    } finally {
      markAsFinished
    }

    if (future.exception.isDefined) {
      future.exception.get match {case (blame, exception) => throw exception}
    }

    future.result.get.asInstanceOf[S3Exchange]
  }

  def markAsFinished = {
    activeRequests.remove(request)
  }

  override def onResponseContent(content: Buffer) {
    super.onResponseContent(content)
  }

  override def onResponseComplete {
    future.completeWithResult(this)
    markAsFinished
  }

  override def onException(ex: Throwable) {
    future.completeWithException(this, ex)
    markAsFinished
  }

  override def onConnectionFailed(ex: Throwable) { onException(ex) }

}
