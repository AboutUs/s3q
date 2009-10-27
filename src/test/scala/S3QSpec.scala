import org.specs._
import org.mockito._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.specs.mock.Mockito

import org.s3q._

import java.util.concurrent.TimeUnit.SECONDS
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import scala.collection.jcl.Conversions._

object S3QSpecification extends Specification with Mockito {

  type Responder = (HttpServletRequest, HttpServletResponse) => Unit

  var responder:Responder = _

  class TestServlet extends HttpServlet
  {
    override def doGet(request: HttpServletRequest, response: HttpServletResponse)
    {
      responder(request, response)
    }
    override def doPut(request: HttpServletRequest, response: HttpServletResponse)
    {
      responder(request, response)
    }
    override def doDelete(request: HttpServletRequest, response: HttpServletResponse)
    {
      responder(request, response)
    }
  }

  val handler = new org.mortbay.jetty.servlet.ServletHandler
  handler.addServletWithMapping(classOf[TestServlet], "/")
  val server = new org.mortbay.jetty.Server(8080)
  server.setHandler(handler)

  server.start

  val client = new S3Client(new S3Config("foo", "bar", 100, 500, "localhost:8080"))

  class TestEnvironment extends Environment {
    override def currentDate: java.util.Date = {
      new java.util.Date(1253576758488L)
    }

    // TODO: set expectations on this
    override def sleep(time: Long) = {}
  }

  Environment.environment = new TestEnvironment
  Environment.environment.logger.setLevel(net.lag.logging.Logger.OFF)

  "With a full queue" should {
    val singleThreadClient = new S3Client(new S3Config("foo", "bar", 1, 500, "localhost:8080"))
    val bucket = new Bucket("test-bucket", singleThreadClient)
    val barrier = new java.util.concurrent.CyclicBarrier(2)

    class Recorder {
      def record(path: String) = None
    }

    "discard the head of the request queue" in {
      val r = mock[Recorder]
      calling {() =>
        bucket.get("1")
        new String(bucket.get("2").data.get) must_== "expected result"
        barrier.await(1, SECONDS)
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        barrier.await(1, SECONDS)
      } withResponse {(request, response) =>
        r.record(request.getRequestURI)
        response.getWriter.print("expected result")
      } call

      r.record("/test-bucket/1") was called.once
      r.record("/test-bucket/2") was called.once
    }
  }

  "A GET request" should {
    val bucket = new Bucket("test-bucket", client)

    "should return a specified header" in {
      calling {() =>
        bucket.get("test-item").header("x-amz-foo") must_== Some("bar")
      } withResponse { (request, response) =>
        response.setHeader("X-Amz-Foo", "bar")
        response.getWriter.print("expected result")
      } call
    }

    "should return None when header does not exist" in {
      calling {() =>
        bucket.get("test-item").header("x-amz-not-here") must beNone
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
      } call
    }

    "should return all headers" in {
      calling {() =>
        val headers = bucket.get("test-item").headers
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
        new String(bucket.get("test-item").data.get) must_== "expected result"
      } withResponse { (request, response) =>
        request.getMethod must_== "GET"
        request.getRequestURI must_== "/test-bucket/test-item"
        request.getHeader("Authorization") must_== "AWS foo:p5KyJTeu/8EYmQqnhOJvz9zS4T4="
        request.getHeader("Date") must_== "Mon, 21 Sep 2009 23:45:58 GMT"
        response.getWriter.print("expected result")
      } call
    }

    "should retry 3 times when a 503 is received" in {
      calling {() =>
        new String(bucket.get("test-item").data.get) must_== "expected result"
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
        bucket.get("test-item").data must beNone
      } withResponse { (request, response) =>
        response.setStatus(404)
      } call

      "should call a callback upon completion" in {
        val barrier = new java.util.concurrent.CyclicBarrier(2)
        calling {() =>
          bucket.get("test-item", { (response) =>
            new String(response.data.get) must_== "expected result"
            barrier.await(1, SECONDS)
          })
        } withResponse { (request, response) =>
          response.getWriter.print("expected result")

        } call

        barrier.await(1, SECONDS)
      }
    }

    "should throw an error if more than 3 503s are received" in {
      calling {() =>
        bucket.get("test-item").data.get must throwAn[S3Exception]
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
    "be successful" in {
      calling {() =>
        bucket.put("test-item", "some-data".getBytes).data must_== Some(null)
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
        bucket.put("test-item", "some-data".getBytes, Map("X-Amz-Boo-Foo-Woo" -> "rulz", "X-Amz-AAAAA" -> "first")).data must_== Some(null)
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
          Map("X-Amz-Boo-Foo-Woo" -> "rulz", "X-Amz-AAAAA" -> "first", "Content-Encoding" -> "gzip")).data must_== Some(null)
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
        bucket.put("test-item", "some-data".getBytes).data
      } withResponse { (request, response) =>
        request.getReader.readLine must_== "some-data"
      } call
    }

    "allow headers to be set" in {
      calling{() =>
        bucket.put("test-item", "some-data".getBytes, Map("X-Amz-Foo" -> "bar")).data
      } withResponse { (request, response) =>
        request.getHeader("X-Amz-Foo") must_== "bar"
      } call
    }

    "should retry 3 times when a 503 is received, even before #data is called" in {
      val barrier = new java.util.concurrent.CyclicBarrier(2)
      var response:S3Response = null
      calling {() =>
        response = bucket.put("test-item", "some-data".getBytes)
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
        bucket.put("test-item", "some-data".getBytes).data.get must throwAn[S3Exception]
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
        new String(bucket.put("test-item", "some-data".getBytes).data.get) must_== "expected result"
      } withResponse { (request, response) =>
        Thread sleep 600
      } withResponse { (request, response) =>
        Thread sleep 600
      } withResponse { (request, response) =>
        Thread sleep 600
      } withResponse { (request, response) =>
        response.getWriter.print("expected result")
      } call
    }
  }

  "A list request" should {
    val bucket = new Bucket("test-bucket", client)

    "should get contents when there is a single page of results" in {
      calling {() =>
        bucket.keys.toList must_== List("foo", "bar")
      } withResponse { (request, response) =>
        request.getMethod must_== "GET"
        request.getRequestURI must_== "/test-bucket/"
        val xml = <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
          <Name>quotes</Name>
          <Prefix></Prefix>
          <Marker></Marker>
          <MaxKeys>40</MaxKeys>
          <IsTruncated>false</IsTruncated>
          <Contents>
            <Key>foo</Key>
            <LastModified>2006-01-01T12:00:00.000Z</LastModified>
            <ETag>&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;</ETag>
            <Size>5</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
              <ID>bcaf1ffd86f41caff1a493dc2ad8c2c281e37522a640e161ca5fb16fd081034f</ID>
              <DisplayName>webfile</DisplayName>
             </Owner>
          </Contents>
          <Contents>
            <Key>bar</Key>
            <LastModified>2006-01-01T12:00:00.000Z</LastModified>
            <ETag>&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;</ETag>
            <Size>4</Size>
            <StorageClass>STANDARD</StorageClass>
             <Owner>
              <ID>bcaf1ffd86f41caff1a493dc2ad8c2c281e37522a640e161ca5fb16fd081034f</ID>
              <DisplayName>webfile</DisplayName>
            </Owner>
         </Contents>
        </ListBucketResult>

        response.getWriter.print(xml.toString)
      } call
    }

    "should get contents for multiple pages of results" in {
      calling {() =>
        bucket.keys.toList must_== List("foo", "bar", "spam", "eggs")
      } withResponse { (request, response) =>
        val xml = <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
          <Name>quotes</Name>
          <Prefix></Prefix>
          <Marker></Marker>
          <MaxKeys>40</MaxKeys>
          <IsTruncated>true</IsTruncated>
          <Contents>
            <Key>foo</Key>
            <LastModified>2006-01-01T12:00:00.000Z</LastModified>
            <ETag>&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;</ETag>
            <Size>5</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
              <ID>bcaf1ffd86f41caff1a493dc2ad8c2c281e37522a640e161ca5fb16fd081034f</ID>
              <DisplayName>webfile</DisplayName>
             </Owner>
          </Contents>
          <Contents>
            <Key>bar</Key>
            <LastModified>2006-01-01T12:00:00.000Z</LastModified>
            <ETag>&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;</ETag>
            <Size>4</Size>
            <StorageClass>STANDARD</StorageClass>
             <Owner>
              <ID>bcaf1ffd86f41caff1a493dc2ad8c2c281e37522a640e161ca5fb16fd081034f</ID>
              <DisplayName>webfile</DisplayName>
            </Owner>
         </Contents>
        </ListBucketResult>

        response.getWriter.print(xml.toString)
      } withResponse { (request, response) =>
        request.getQueryString must_== "max_keys=1000&marker=bar"
        val xml = <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
          <Name>quotes</Name>
          <Prefix></Prefix>
          <Marker></Marker>
          <MaxKeys>40</MaxKeys>
          <IsTruncated>false</IsTruncated>
          <Contents>
            <Key>spam</Key>
            <LastModified>2006-01-01T12:00:00.000Z</LastModified>
            <ETag>&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;</ETag>
            <Size>5</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
              <ID>bcaf1ffd86f41caff1a493dc2ad8c2c281e37522a640e161ca5fb16fd081034f</ID>
              <DisplayName>webfile</DisplayName>
             </Owner>
          </Contents>
          <Contents>
            <Key>eggs</Key>
            <LastModified>2006-01-01T12:00:00.000Z</LastModified>
            <ETag>&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;</ETag>
            <Size>4</Size>
            <StorageClass>STANDARD</StorageClass>
             <Owner>
              <ID>bcaf1ffd86f41caff1a493dc2ad8c2c281e37522a640e161ca5fb16fd081034f</ID>
              <DisplayName>webfile</DisplayName>
            </Owner>
         </Contents>
        </ListBucketResult>

        response.getWriter.print(xml.toString)
      } call
    }

  }

  "A DELETE Request" should {
    val bucket = new Bucket("test-bucket", client)
    "be successful" in {
      calling {() =>
        bucket.delete("test-item").data must_== Some(null)
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

    "should retry 3 times when a 503 is received" in {
      calling {() =>
        new String(bucket.delete("test-item").data.get) must_== "expected result"
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
        bucket.delete("test-item").data.get must throwAn[S3Exception]
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

  def calling(requestBlock:() => Unit): ClientExpectation = {
    new ClientExpectation(requestBlock)
  }

  case class ClientExpectation(requestBlock:() => Unit) {
    var responderCaught:Option[Exception] = None
    val responders:scala.collection.mutable.Queue[Responder] = new scala.collection.mutable.Queue

    def withResponse(expectationBlock: Responder): ClientExpectation = {
      responders += expectationBlock
      this
    }

    def call {
      responder = (request, response) => {
        try {
          val expectationBlock = responders.dequeue
          expectationBlock(request, response)
        } catch {
          case e:Exception => responderCaught = Some(e)
        }
      }


      try {
        requestBlock()
      } catch {
        case e:Exception => {
          raiseIfResponderCaught
          throw e
        }
      }

      raiseIfResponderCaught
    }

    def raiseIfResponderCaught {
      responderCaught match {
        case Some(exception) => throw exception
        case None => {}
      }
    }


  }



}
