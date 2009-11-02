package org.s3q.concurrent

import java.util.concurrent.{FutureTask, Callable, TimeUnit, TimeoutException}

// HACK: FutureTask needs to a "Callable" to be constructed, even if we're disabling "run" and
// fulfilling the Promise externally.

class NullCallable[T] extends Callable[T] {
  def call():T = { null.asInstanceOf[T] }
}

class Future[T](timeout: Long, unit: TimeUnit) extends FutureTask(new NullCallable[T]) {
  class AlreadyFulfilledException extends Exception

  def fulfill(result: T) = synchronized {
    isDone match {
      case false => set(result)
      case true => throw new AlreadyFulfilledException
    }
  }

  def apply():Either[TimeoutException, T] = {
    try {
      Right(get(timeout, unit))
    } catch {
      case exception:TimeoutException => Left(exception)
    }
  }

  override def run = {}
}