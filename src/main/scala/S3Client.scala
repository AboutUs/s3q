package org.s3q

import concurrent.Future

import org.xlightweb.client.HttpClient
import org.xlightweb.{GetRequest, PutRequest, DeleteRequest, IHttpResponse, IHttpResponseHandler}

import java.util.concurrent.TimeUnit._

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

case class S3Config(args: (Symbol, Any)*) {

    val defaults = Map(
      'maxConcurrency -> 500,
      'timeout -> 6000L,
      'hostname -> "s3.amazonaws.com",
      'evictionPolicy -> AppendPolicy,
      'retry -> false)

    val config = args.foldLeft(defaults) { case (m, (k, v)) =>
      m + (k -> v)
    }

    val accessKeyId = config('accessKeyId).asInstanceOf[String]
    val secretAccessKey = config('secretAccessKey).asInstanceOf[String]
    val maxConcurrency = config('maxConcurrency).asInstanceOf[Int]
    val timeout = config('timeout).asInstanceOf[Long]
    val hostname = config('hostname).asInstanceOf[String]
    val evictionPolicy = config('evictionPolicy).asInstanceOf[Eviction]
    val retry = config('retry).asInstanceOf[Boolean]
}

object Types {
  type ResponseFuture = Future[Either[Exception, S3Response]]
  type ResponseCallback = (Either[Exception, HttpResponse]) => Unit
}


class S3Client(val config:S3Config) {
  private val log = Logger.get

  val activeRequests = new ArrayBlockingQueue[S3RequestHandler](config.maxConcurrency)

  val client = new HttpClient

  val MAX_ATTEMPTS = 3

  def execute(request: S3Request): S3ResponseFuture = {
    val future = new Types.ResponseFuture(config.timeout, MILLISECONDS)

    execute(request, MAX_ATTEMPTS, {(httpResponse) =>
      val response = process(request, httpResponse)
      future.fulfill(response)
      request.callback(response)
    })

    new S3ResponseFuture(request, this, future)
  }

  def process(request: S3Request, response: Either[Exception, HttpResponse]) = response match {
    case Right(response) => response.isOk match {
      case true => Right(request.response(response))
      case false => Left(BadResponseCode(response.status, response.bodyString))
    }
    case Left(ex) => Left(ex)
  }


  def execute(request: S3Request, attemptsLeft: Int, callback:Types.ResponseCallback):Unit = {
    val handler = new S3RequestHandler(this, request, activeRequests, attemptsLeft, callback)

    log.debug("Queuing request... %s slots remaining", activeRequests.remainingCapacity())

    executeOnQueue(handler)
  }

  def queueFull = activeRequests.remainingCapacity() == 0

  def executeOnQueue(handler: S3RequestHandler) {
    executeExchange(handler)
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
      case "GET"      => new GetRequest(request.url)
      case "DELETE"   => new DeleteRequest(request.url)
      case "PUT"      => new PutRequest(request.url, request.contentType, request.body.get)
    }

    request.headers.foreach { case (key, value) =>
      cRequest.addHeader(key, value)
    }


    cRequest
  }

}

class S3RequestHandler(val client: S3Client, val request: S3Request, activeRequests: BlockingQueue[S3RequestHandler], val attemptsLeft:Int,
  val callback:Types.ResponseCallback)
  extends IHttpResponseHandler
{

  def markAsFinished = {
    activeRequests.remove(this)
  }

  override def onException(exception: java.io.IOException) = {
    markAsFinished
    maybeRetry(Left(exception))
    callback(Left(exception))
  }

  override def onResponse(httpResponse: IHttpResponse) = {
    markAsFinished
    val response = new HttpResponse(httpResponse)

    response.isOk match {
      case true => {
        callback(Right(response))
      }
      case false => {
        maybeRetry(Right(response))
      }
    }

  }

  def maybeRetry(value: Either[Exception, HttpResponse]) = {
    attemptsLeft match {
      case 0 => callback(value)
      case _ => client.execute(request, attemptsLeft - 1, callback)
    }
  }

}
