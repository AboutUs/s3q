package org.s3q.concurrent

import java.util.concurrent.{FutureTask, Callable}

// HACK: FutureTask needs to a "Callable" to be constructed, even if we're disabling "run" and
// fulfilling the Promise externally.

class NullCallable[T] extends Callable[T] {
  def call():T = { null.asInstanceOf[T] }
}

class Future[T] extends FutureTask(new NullCallable[T]) {
  def fulfill(result: T) = {
    set(result)
  }

  override def run = {}
}