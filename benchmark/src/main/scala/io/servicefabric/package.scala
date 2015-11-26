package io

import java.util.concurrent.Callable

import com.codahale.metrics.{MetricRegistry, Timer}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContext, Promise, Future}

/**
  * Created by mlisniak on 25/11/15.
  */
package object servicefabric {

  implicit def listenableFuture2Future[T](listenableFuture: ListenableFuture[T]) : Future[T] = {
    toScalaFuture(listenableFuture)
  }

  private def toScalaFuture[T](listenableFuture: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback(listenableFuture, new FutureCallback[T] {
      override def onFailure(t: Throwable): Unit = {
        p failure t
      }

      override def onSuccess(result: T): Unit = {
        p success result
      }
    })
    p.future
  }

  implicit class RichListenableFuture[T](lf : ListenableFuture[T]){
    def asScala : Future[T] = {
      toScalaFuture(lf)
    }
  }
  implicit class RichTimer(timer: Timer) {
    def timed[T](x: => T): T = timer.time(new Callable[T] {
      override def call(): T = x
    })

    def timed[T](future: Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val context = timer.time()
      future onComplete { _ => context.close() }
      future
    }
  }
  def timer(klass: Class[_], names: String*)(implicit metrics: MetricRegistry) = metrics.timer(MetricRegistry.name(klass, names: _*))
}
