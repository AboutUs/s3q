import org.specs._
import org.mockito._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.specs.mock.Mockito

import org.s3q._
import org.s3q.specs._
import org.s3q.specs.Common._

import java.util.concurrent.TimeUnit.SECONDS
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import scala.collection.jcl.Conversions._

object S3QSpecification extends Specification with Mockito {

  var responder:Responder = _

/*  val server = server(responder)*/
/*  server.start*/
  implicit val server = startTestServer


  val client = new S3Client(new S3Config("foo", "bar", 100, 500, "localhost:8080"))

  Environment.environment = new TestEnvironment
  Environment.environment.logger.setLevel(net.lag.logging.Logger.WARNING)

  "A GET request" should {
    val bucket = new Bucket("test-bucket", client)

    "should return a specified header" in {
      calling {() =>
        bucket.get("test-item").response.right.get.header("x-amz-foo") must_== Some("bar")
      } withResponse { (request, response) =>
        response.setHeader("X-Amz-Foo", "bar")
        response.getWriter.print("expected result")
      } call
    }

    "should return None when header does not exist" in {
      calling {() =>
        bucket.get("test-item").response.right.get.header("x-amz-not-here") must beNone
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
      } call
    }

    "should return all headers" in {
      calling {() =>
        val headers = bucket.get("test-item").response.right.get.headers
        headers must haveKey("x-amz-foo")
        headers must haveKey("x-amz-spam")
      } withResponse { (request, response) =>
        response.setHeader("X-Amz-Foo", "bar")
        response.setHeader("X-Amz-Spam", "eggs")
        response.getWriter.print("expected result")
      } call
    }

    "should be successful" in {
      calling {() =>
        bucket.get("test-item").response.right.get.dataString must_== Some("expected result")
      } withResponse { (request, response) =>
        request.getMethod must_== "GET"
        request.getRequestURI must_== "/test-bucket/test-item"
        request.getHeader("Authorization") must_== "AWS foo:p5KyJTeu/8EYmQqnhOJvz9zS4T4="
        request.getHeader("Date") must_== "Mon, 21 Sep 2009 23:45:58 GMT"
        response.getWriter.print("expected result")
        response.getWriter.flush
      } call
    }

    "should not retry if it a 404 is received" in {
      calling {() =>
        bucket.get("test-item").response.right.get.data must beNone
      } withResponse { (request, response) =>
        response.setStatus(404)
      } call
    }

    "should call a callback upon completion" in {
      val responseReceived = new java.util.concurrent.CyclicBarrier(2)
      calling {() =>
        bucket.get("test-item", { (response) =>
          response.right.get.dataString must_== Some("expected result")
          responseReceived.await(1, SECONDS)
        })
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
        response.getWriter.flush
      } call

      responseReceived.await(1, SECONDS)
    }

  }

  "A PUT Request" should {
    val bucket = new Bucket("test-bucket", client)
    "be successful" in {
      calling {() =>
        bucket.put("test-item", "some-data".getBytes).response
      } withResponse { (request, response) =>
        request.getMethod must_== "PUT"
        request.getRequestURI must_== "/test-bucket/test-item"
        request.getHeader("Authorization") must_== "AWS foo:79H1wpxHvrH5mJfoMi33hgzYupc="
        request.getHeader("Date") must_== "Mon, 21 Sep 2009 23:45:58 GMT"
        request.getHeader("Content-MD5") must_== "MVaNlMH/BQXRc8prXMPPSQ=="
        response.setStatus(200)
      } call
    }

    "take into account arbitrary X-Amz headers when authorizing" in {
      calling {() =>
        bucket.put("test-item", "some-data".getBytes, Map("X-Amz-Boo-Foo-Woo" -> "rulz", "X-Amz-AAAAA" -> "first")).response
      } withResponse { (request, response) =>
        request.getMethod must_== "PUT"
        request.getRequestURI must_== "/test-bucket/test-item"
        request.getHeader("Authorization") must_== "AWS foo:ckj5nL5TK1pLAinJG/hvEQXyrcI="
        request.getHeader("Date") must_== "Mon, 21 Sep 2009 23:45:58 GMT"
        request.getHeader("Content-MD5") must_== "MVaNlMH/BQXRc8prXMPPSQ=="
        response.setStatus(200)
      } call
    }

    "take into account arbitrary X-Amz headers but no other headers when authorizing" in {
      calling {() =>
        bucket.put("test-item", "some-data".getBytes,
          Map("X-Amz-Boo-Foo-Woo" -> "rulz", "X-Amz-AAAAA" -> "first", "Content-Encoding" -> "gzip")).response
      } withResponse { (request, response) =>
        request.getMethod must_== "PUT"
        request.getRequestURI must_== "/test-bucket/test-item"
        request.getHeader("Authorization") must_== "AWS foo:ckj5nL5TK1pLAinJG/hvEQXyrcI="
        request.getHeader("Date") must_== "Mon, 21 Sep 2009 23:45:58 GMT"
        request.getHeader("Content-MD5") must_== "MVaNlMH/BQXRc8prXMPPSQ=="
        response.setStatus(200)
      } call
    }

    "send data in the body of the request" in {
      calling{() =>
        bucket.put("test-item", "some-data".getBytes).response
      } withResponse { (request, response) =>
        request.getReader.readLine must_== "some-data"
      } call
    }

    "allow headers to be set" in {
      calling{() =>
        bucket.put("test-item", "some-data".getBytes, Map("X-Amz-Foo" -> "bar")).response
      } withResponse { (request, response) =>
        request.getHeader("X-Amz-Foo") must_== "bar"
      } call
    }

  }


  "A DELETE Request" should {
    val bucket = new Bucket("test-bucket", client)
    "be successful" in {
      calling {() =>
        bucket.delete("test-item").response
      } withResponse { (request, response) =>
        request.getMethod must_== "DELETE"
        request.getRequestURI must_== "/test-bucket/test-item"
        request.getHeader("Authorization") must_== "AWS foo:y89SjNCHD9fBl9E9SNmgvCfozJg="
        request.getHeader("Date") must_== "Mon, 21 Sep 2009 23:45:58 GMT"
        response.setStatus(204)
      } call
    }
    "have a valid url" in {
      val request = new S3Delete(client, "test-bucket", "path")
      request.url must_== "http://localhost:8080/test-bucket/path"
    }

  }



}
