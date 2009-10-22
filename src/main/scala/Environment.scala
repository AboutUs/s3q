package org.s3q
import net.lag.configgy.Configgy
import net.lag.logging.Logger

object Environment {
  var environment:Environment = new Environment
  def env = { environment }
}

class Environment {
  def currentDate:java.util.Date = {
    new java.util.Date(System.currentTimeMillis)
  }

  def sleep(time: Long) = {
    Thread.sleep(time)
  }

  lazy val logger = Logger("S3Q")
}
