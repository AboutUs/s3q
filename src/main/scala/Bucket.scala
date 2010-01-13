package org.s3q

class Bucket(name: String, client: S3Client) {
  val bufferSize = 501
  val refillWhen = bufferSize / 2

  def items(firstKey: Option[String]) = {
    val keyIterator = keys(firstKey).elements
    val buffer = new scala.collection.mutable.ListBuffer[(String, S3Response)]

    def fillBuffer = {
      val keys = keyIterator.take(bufferSize).toList
      buffer ++= keys.map { key => (key, get(key)) }
    }

    new Iterator[(String, S3Response)] {
      def hasNext = { !buffer.isEmpty || keyIterator.hasNext }

      def next = {
        if (keyIterator.hasNext && buffer.length < bufferSize ) { fillBuffer }
        buffer.remove(0)
      }
    }
  }

  def items(firstKey: String):Iterator[(String, S3Response)] = items(Some(firstKey))

  def items:Iterator[(String, S3Response)] = items(None)

  private def keyStreams(first_key: Option[String]) = {
    val MAX_BATCH = 1000
    var marker: Option[String] = first_key
    var done = false

    new Iterator[Iterable[String]] {
      def hasNext = !done

      def next = {
        val response = client.execute(new S3List(client, name, MAX_BATCH, marker))
        val items = response.items
        marker = items.lastOption
        done = !response.isTruncated

        items
      }
    }
  }

  def keys:Iterable[String] = keys(None)

  def keys(marker: String):Iterable[String] = keys(Some(marker))

  def keys(marker: Option[String]) = {
    Stream.concat(keyStreams(marker).map(_.toStream))
  }

  def get(key: String) = {
    client.execute(new S3Get(client, name, key))
  }

  def get(key: String, withCallback: (Option[S3Response]) => Unit) = {
    client.execute(new S3Get(client, name, key) {
      override def callback(request: Option[S3Response]) = withCallback(request)
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
