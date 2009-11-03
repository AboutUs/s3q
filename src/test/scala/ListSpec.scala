import org.specs._
import org.mockito._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.specs.mock.Mockito

import org.s3q._
import org.s3q.specs._
import org.s3q.specs.Common._


object ListSpecification extends Specification with Mockito {

  var responder:Responder = _

  implicit val server = startTestServer

  doAfterSpec { server.stop }

  val client = new S3Client(
    new S3Config('accessKeyId -> "foo",
                 'secretAccessKey -> "bar",
                 'hostname -> "localhost:8080",
                 'timeout -> 100L))

  Environment.environment = new TestEnvironment
  Environment.environment.logger.setLevel(net.lag.logging.Logger.WARNING)

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

    "should be able to start listing at a key other than the first" in {
      calling {() =>
        bucket.keys("foo")
      } withResponse { (request, response) =>
        request.getQueryString must_== "max_keys=1000&marker=foo"
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
}