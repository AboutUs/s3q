import sbt._

class S3QProject(info: ProjectInfo) extends DefaultProject(info)
{
  val specs = "org.scala-tools.testing" % "specs" % "1.5.0" // 1.6 breaks the specs - all pending
  val junit = "junit" % "junit" % "4.7"
  val mockito = "org.mockito" % "mockito-all" % "1.7"
  val httpserver = "org.mortbay.jetty" % "jetty" % "6.1.20"

  val httpclient = "org.xlightweb" % "xlightweb" % "2.9"
  val codec = "commons-codec" % "commons-codec" % "1.3"
  val lagDotNet = "lag.net Respository" at "http://www.lag.net/repo/"
  val configgy = "net.lag" % "configgy" % "1.4"
}
