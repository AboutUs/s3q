package org.s3q

import scala.collection.jcl.Conversions._

class HttpResponse(val response: org.xlightweb.IHttpResponse) {
  lazy val status = response.getStatus
  lazy val body = response.getBlockingBody.readBytes
  lazy val bodyString = body.toString

  lazy val isOk = status match {
    case s if (200 to 299) contains s => true
    case 404 => true
    case _ => false
  }

  lazy val headers = {
    response.getHeaderNameSet.
      foldLeft(Map[String, String]()) { (m, key) =>
        m + (key.toLowerCase -> response.getHeader(key))
      }
  }
}