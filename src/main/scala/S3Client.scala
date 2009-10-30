package org.s3q

import com.aboutus.auctors.kernel.reactor.{DefaultCompletableFutureResult, FutureTimeoutException}

import org.xlightweb.client.HttpClient
import org.xlightweb.{GetRequest, PutRequest, FutureResponseHandler, IHttpResponse}

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

  val activeRequests = new ArrayBlockingQueue[S3RequestHandler](config.maxConcurrency)

  val client = new HttpClient

  def execute(request: S3Request): S3Response = {
    val handler = new S3RequestHandler(this, request, activeRequests)

    log.debug("Queuing request... %s slots remaining", activeRequests.remainingCapacity())

    executeOnQueue(handler).response
  }

  def execute(request: S3List): S3ListResponse = {
    execute(request.asInstanceOf[S3Request]).asInstanceOf[S3ListResponse]
  }

  def queueFull = activeRequests.remainingCapacity() == 0

  def executeOnQueue(handler: S3RequestHandler): S3RequestHandler = {
    if (queueFull) {
      val evicted = evictHeadFromQueue
      executeExchange(handler)
      config.evictionPolicy match {
        case DiscardPolicy =>
        case AppendPolicy => {
          evicted match {
            case Some(ex) => // ex.response.retry(new Exception)
            case None =>
          }
        }
      }
    } else {
      executeExchange(handler)
    }

    handler
  }

  def executeExchange(handler: S3RequestHandler): S3RequestHandler = {
    activeRequests.put(handler)

    client.send(clientRequest(handler.request), handler)

    handler
  }

  def evictHeadFromQueue: Option[S3RequestHandler] = {
    activeRequests.poll match {
      case ex: S3RequestHandler => {
        log.warning("Eviction on full queue (Policy: " + config.evictionPolicy.name + "): " + ex.request.bucket + " " + ex.request.path)
        Some(ex)
      }
      case null => None
    }
  }

  def clientRequest(request: S3Request) = {
    val cRequest = request.verb match {
      case "GET" => new GetRequest(request.url)
      case "PUT" => { new PutRequest(request.url, "application/text", request.body.get) }
    }

    request.headers.foreach { case (key, value) =>
      cRequest.addHeader(key, value)
    }


    cRequest
  }

}

class S3RequestHandler(val client: S3Client, val request: S3Request, activeRequests: BlockingQueue[S3RequestHandler])
  extends FutureResponseHandler
{
  lazy val whenFinished = {
    get(client.config.timeout, java.util.concurrent.TimeUnit.MILLISECONDS)
  }

  lazy val response = {
    request.response(this)
  }

  def markAsFinished = {
    activeRequests.remove(this)
  }

/*  override def onException(exception: java.io.IOException) = {
    super.onException(exception)
    markAsFinished
  }

  override def onResponse(httpResponse: IHttpResponse) = {
    super.onResponse(httpResponse)
    markAsFinished
    response.verify
    request.callback(response)
  }
*/
}
