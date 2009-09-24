package org.s3q

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
}