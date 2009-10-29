package org.s3q

import com.aboutus.auctors.kernel.reactor.{DefaultCompletableFutureResult, FutureTimeoutException}

import org.mortbay.jetty.client.ContentExchange
import org.mortbay.io.Buffer

import java.util.concurrent._
import net.lag.configgy.Configgy
import net.lag.logging.Logger

abstract case class Eviction() {
  val name: String
}
case object Eviction {
  implicit def string2eviction(name: String): Eviction = {
    name match {
      case "discard" => DiscardPolicy
      case "append" => AppendPolicy
      case _ => throw new IllegalArgumentException("Invalid eviction policy")
    }
  }
}
case object DiscardPolicy extends Eviction {
  override val name = "discard"
}
case object AppendPolicy extends Eviction {
  override val name = "append"
}

case class S3Config(
  val accessKeyId: String, val secretAccessKey: String,
  val maxConcurrency:Int, val timeout:Int, val hostname:String,
  val evictionPolicy: Eviction
) {
    def this(
      accessKeyId: String, secretAccessKey: String, maxConcurrency:Int, timeout:Int, hostname:String
    ) = this(accessKeyId, secretAccessKey, maxConcurrency, timeout, hostname, AppendPolicy)

    def this(
      accessKeyId: String, secretAccessKey: String, maxConcurrency:Int, timeout:Int
    ) = this(accessKeyId, secretAccessKey, maxConcurrency, timeout, "s3.amazonaws.com")

    def this(
      accessKeyId: String, secretAccessKey: String, maxConcurrency:Int
    ) = this(accessKeyId, secretAccessKey, maxConcurrency, 6000)

    def this(accessKeyId: String, secretAccessKey: String) =
      this(accessKeyId, secretAccessKey, 500)

}

class S3Client(val config:S3Config) {
  private val log = Logger.get

  val activeRequests = new ArrayBlockingQueue[S3Exchange](config.maxConcurrency)

  val client = new org.mortbay.jetty.client.HttpClient
  client.setConnectorType(org.mortbay.jetty.client.HttpClient.CONNECTOR_SELECT_CHANNEL)

  client.start

  def execute(request: S3Request): S3Response = {
    val exchange = new S3Exchange(this, request, activeRequests)
    log.debug("Queuing request... %s slots remaining", activeRequests.remainingCapacity())
    executeOnQueue(exchange).response
  }

  def execute(request: S3List): S3ListResponse = {
    execute(request.asInstanceOf[S3Request]).asInstanceOf[S3ListResponse]
  }

  def queueFull = activeRequests.remainingCapacity() == 0

  def executeOnQueue(exchange: S3Exchange): S3Exchange = {
    if (queueFull) {
      val evicted = evictHeadFromQueue
      executeExchange(exchange)

      config.evictionPolicy match {
        case DiscardPolicy =>
        case AppendPolicy => {
          evicted match {
            case Some(ex) => ex.response.retry(new Exception)
            case None =>
          }
        }
      }
    } else {
      executeExchange(exchange)
    }

    exchange
  }

  def executeExchange(exchange: S3Exchange): S3Exchange = {
    activeRequests.put(exchange)
    client.send(exchange)

    exchange
  }

  def evictHeadFromQueue: Option[S3Exchange] = {
    activeRequests.poll match {
      case ex: S3Exchange => {
        log.warning("Eviction on full queue (Policy: " + config.evictionPolicy.name + "): " + ex.request.bucket + " " + ex.request.path)
        Some(ex)
      }
      case null => None
    }
  }

}

class S3Exchange(val client: S3Client, val request: S3Request,
  activeRequests: BlockingQueue[S3Exchange]) extends ContentExchange {
  setMethod(request.verb)
  setURL(request.url)
  request.body match {
    case Some(string) => setRequestContent(string)
    case None => ()
  }

  for ((key, value) <- request.headers) {
    setRequestHeader(key, value)
  }

  lazy val response: S3Response = {
    request.response(this)
  }

  var responseHeaders = new scala.collection.mutable.HashMap[String, String]

  override def onResponseHeader(key: Buffer, value: Buffer) = {
    super.onResponseHeader(key, value)
    responseHeaders += key.toString.toLowerCase -> value.toString
  }

  val future = new DefaultCompletableFutureResult(client.config.timeout)

  def status = getResponseStatus

  def get: Either[Throwable, S3Exchange] = {
    try {
      future.await
    }
    catch {
      case e: FutureTimeoutException => return Left(new TimeoutException)
    } finally {
      markAsFinished
    }

    if (future.exception.isDefined) {
      future.exception.get match {case (blame, exception) => return Left(exception)}
    }

    Right(future.result.get.asInstanceOf[S3Exchange])
  }

  def markAsFinished = {
    activeRequests.remove(this)
  }

  override def onResponseContent(content: Buffer) {
    super.onResponseContent(content)
  }

  override def onResponseComplete {
    future.completeWithResult(this)
    markAsFinished
    response.verify
    request.callback(Some(response))
  }

  override def onException(ex: Throwable) {
    future.completeWithException(this, ex)
    markAsFinished
    request.callback(None)
  }

  override def onConnectionFailed(ex: Throwable) { onException(ex) }

}
