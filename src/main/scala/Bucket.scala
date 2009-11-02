package org.s3q

class Bucket(name: String, client: S3Client) {
  val bufferSize = 501
  val refillWhen = bufferSize / 2

//  def items = {
//    val keyIterator = keys.elements
//    val buffer = new scala.collection.mutable.ListBuffer[(String, S3ResponseFuture)]
//
//    def fillBuffer = {
//      //val keys = keyIterator.take(bufferSize).toList
//      // buffer ++= keys.map { key => (key, get(key)) }
//    }
//
//    new Iterator[(String, S3ResponseFuture)] {
//      def hasNext = { !buffer.isEmpty || keyIterator.hasNext }
//
//      def next = {
//        if (keyIterator.hasNext && buffer.length < bufferSize ) { fillBuffer }
//        buffer.remove(0)
//      }
//    }
//  }
//
//  private def keyStreams(first_key: Option[String]) = {
//    val MAX_BATCH = 1000
//    var marker: Option[String] = first_key
//    var done = false
//
//    new Iterator[Iterable[String]] {
//      def hasNext = !done
//
//      def next = {
//        val response = client.execute(new S3List(client, name, MAX_BATCH, marker))
//        val items = response.items
//        marker = items.lastOption
//        done = !response.isTruncated
//
//        items
//      }
//    }
//  }
//
//  def keys:Iterable[String] = {
//    Stream.concat(keyStreams(None).map(_.toStream))
//  }
//
//  def keys(marker: String):Iterable[String] = {
//    Stream.concat(keyStreams(Some(marker)).map(_.toStream))
//  }

  def get(key: String) = {
    client.execute(new S3Get(client, name, key))
  }

  def get(key: String, withCallback: (Either[Throwable, S3Response]) => Unit) = {
    client.execute(new S3Get(client, name, key) {
      override def callback(request: Either[Throwable, S3Response]) = withCallback(request)
    })
  }

  def put(key: String, data: Array[Byte]) = {
    client.execute(new S3Put(client, name, key, data))
  }

  def put(key: String, data: Array[Byte], headers: Map[String, String]) = {
    client.execute(new S3Put(client, name, key, data, headers))
  }

  def delete(key: String) = {
    client.execute(new S3Delete(client, name, key))
  }
}
