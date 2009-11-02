package org.s3q.specs

import org.mortbay.jetty
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import scala.collection.jcl.Conversions._


class TestServer {
  var responder: Common.Responder = _

  class Handler extends jetty.handler.AbstractHandler {

    def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      responder(request, response)
    }

  }

  val server = new org.mortbay.jetty.Server(8080)
  server.setHandler(new Handler)

  server
}


class TestEnvironment extends Environment {
  override def currentDate: java.util.Date = {
    new java.util.Date(1253576758488L)
  }

  // TODO: set expectations on this
  override def sleep(time: Long) = {}
}

class Recorder {
  def record(path: String) = None
}

object Common {

  def startTestServer = {
    val testServer = new TestServer

    testServer.server.start

    testServer
  }

  type Responder = (HttpServletRequest, HttpServletResponse) => Unit

  def calling(requestBlock:() => Unit)(implicit server: TestServer): ClientExpectation = {
    new ClientExpectation(requestBlock, server)
  }

  case class ClientExpectation(requestBlock:() => Unit, server: TestServer) {
    var responderCaught:Option[Exception] = None
    val responders:scala.collection.mutable.Queue[Responder] = new scala.collection.mutable.Queue

    def withResponse(expectationBlock: Responder): ClientExpectation = {
      responders += expectationBlock
      this
    }

    def call {
      server.responder = (request, response) => {
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