import org.specs._
import org.s3q._


import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import scala.collection.jcl.Conversions._

object S3QSpecification extends Specification  {

  type Responder = (HttpServletRequest, HttpServletResponse) => Unit

  var responder:Responder = _

  class TestServlet extends HttpServlet
  {
    override def doGet(request: HttpServletRequest, response: HttpServletResponse)
    {
      responder(request, response)
    }
  }

  val handler = new org.mortbay.jetty.servlet.ServletHandler
  handler.addServletWithMapping(classOf[TestServlet], "/")
  val server = new org.mortbay.jetty.Server(8080)
  server.setHandler(handler)

  server.start

  val client = new S3Client(new S3Config("foo", "bar", 100, "localhost:8080"))

  class TestEnvironment extends Environment {
    override def currentDate: java.util.Date = {
      new java.util.Date(1253576758488L)
    }
  }

  Environment.environment = new TestEnvironment


  "should issue a simple GET request" in {
    calling {() =>
      val bucket = new Bucket("test-bucket", client)
      bucket.get("test-item").data must_== "expected result"
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
      val bucket = new Bucket("test-bucket", client)
      bucket.get("test-item").data must_== "expected result"
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
      val bucket = new Bucket("test-bucket", client)
      bucket.get("test-item").data must throwAn[S3Exception]
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
