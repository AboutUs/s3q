import org.specs._
import org.mockito._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.specs.mock.Mockito

import org.s3q._
import org.s3q.specs._
import org.s3q.specs.Common._

import java.util.concurrent.TimeUnit.SECONDS
import scala.collection.jcl.Conversions._

object RecoverySpecification extends Specification with Mockito {

  var responder:Responder = _

  implicit val server = startTestServer

  doAfterSpec { server.stop }

  val client = new S3Client(new S3Config("foo", "bar", 100, 500, "localhost:8080"))

  Environment.environment = new TestEnvironment
  Environment.environment.logger.setLevel(net.lag.logging.Logger.WARNING)

  "With a full queue using discard policy" should {
    val singleThreadClient = new S3Client(new S3Config("foo", "bar", 1, 500, "localhost:8080", "discard"))
    val bucket = new Bucket("test-bucket", singleThreadClient)
    val barrier = new java.util.concurrent.CyclicBarrier(2)

    "discard the head of the request queue" in {
      val r = mock[Recorder]

      val firstRequestDone = new java.util.concurrent.CyclicBarrier(2)
      calling {() =>
        bucket.get("1")
        firstRequestDone.await(1, SECONDS)
        bucket.get("2").response.right.get.dataString must_== Some("expected result")
        barrier.await(1, SECONDS)
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        firstRequestDone.await(1, SECONDS)
        barrier.await(1, SECONDS)
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        response.getWriter.print("expected result")
      } call

      (r.record("/test-bucket/1") on r) then
      (r.record("/test-bucket/2") on r) were calledInOrder
    }
  }

  "With a full queue using append policy" should {
    val singleThreadClient = new S3Client(new S3Config("foo", "bar", 1, 500, "localhost:8080", "append"))
    val bucket = new Bucket("test-bucket", singleThreadClient)
    val barrier = new java.util.concurrent.CyclicBarrier(2)

    "append the head of the request queue after the new request" in {
      val r = mock[Recorder]
      calling {() =>
        bucket.get("1")
        bucket.get("2").response.right.get.dataString must_== "expected result"
        barrier.await(1, SECONDS)
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        barrier.await(1, SECONDS)
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        response.getWriter.print("expected result")
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        response.getWriter.print("expected result")
      } call

      (r.record("/test-bucket/1") on r) then
      (r.record("/test-bucket/2") on r) then
      (r.record("/test-bucket/1") on r) were calledInOrder
    }
  }


  "A GET request" should {
    val bucket = new Bucket("test-bucket", client)

    "should retry 3 times when a 503 is received" in {
      calling {() =>
        bucket.get("test-item").response.right.get.dataString must_== Some("expected result")
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
      } call
    }

    "should not retry if it a 404 is received" in {
      calling {() =>
        bucket.get("test-item").response.right.get.data must beNone
      } withResponse { (request, response) =>
        response.setStatus(404)
      } call

    }

    "should throw an error if more than 3 503s are received" in {
      calling {() =>
        bucket.get("test-item").response must_== Left(BadResponseCode(503))
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } call

    }
  }

  "A PUT Request" should {
    val bucket = new Bucket("test-bucket", client)

    "should retry 3 times when a 503 is received, even before #data is called" in {
      val barrier = new java.util.concurrent.CyclicBarrier(2)
      var response:S3Response = null
      calling {() =>
        response = bucket.put("test-item", "some-data".getBytes).response.right.get
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
        barrier.await(1, SECONDS)
      } call

      barrier.await(1, SECONDS)
      new String(response.data.get) must_== "expected result"
    }

    "should throw an error if more than 3 503s are received" in {
      calling {() =>
        bucket.put("test-item", "some-data".getBytes).response must_== Left(BadResponseCode(503))
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } call
    }

    "should retry three times if it receives a timeout" in {
      calling {() =>
        println("calling")
        val o = bucket.put("test-item", "some-data".getBytes).response.right.get
        println("called")
        o.dataString must_== Some("expected result")
      } withResponse { (request, response) =>
        println("first")
        Thread sleep 600
      } withResponse { (request, response) =>
              println("second")
        Thread sleep 600
      } withResponse { (request, response) =>
        println("third")
        Thread sleep 600
      } withResponse { (request, response) =>
        println("last")
        response.getWriter.print("expected result")
      } call
    }
  }

  "A DELETE Request" should {
    val bucket = new Bucket("test-bucket", client)

    "should retry 3 times when a 503 is received" in {
      calling {() =>
        bucket.delete("test-item").response.right.get.dataString must_== Some("expected result")
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
      } call
    }

    "should throw an error if more than 3 503s are received" in {
      calling {() =>
        bucket.delete("test-item").response must_== Left(BadResponseCode(503))
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } withResponse { (request, response) =>
        response.setStatus(503)
      } call

    }
  }

}
