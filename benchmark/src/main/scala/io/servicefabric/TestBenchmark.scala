package io.servicefabric

import java.io.File

import com.codahale.metrics.{CsvReporter, SharedMetricRegistries, MetricRegistry}
import io.servicefabric.cluster.{ICluster, Cluster}
import io.servicefabric.transport.Message
import rx.observable.ListenableFutureObservable

import scala.concurrent.Future

/**
  * Created by mlisniak on 25/11/15.
  */
object TestBenchmark {

  implicit val metrics = SharedMetricRegistries.getOrCreate("test")
  CsvReporter.forRegistry(metrics).build(new File("."))
  val timer1 = timer(TestBenchmark.this.getClass, "test name")


  def main(args: Array[String]) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    for(j <- 1 to 1) {
      val nodes = for (i <- 1 to 1) yield Cluster.join().asScala


      val listening = nodes.map {
        nodeFut =>
          nodeFut.flatMap {
            cluster =>
              ListenableFutureObservable.to(cluster.gossip().listen())
          }
      }
      //val listening2 : Seq[Future[Message]]= for (nodeFut : Future[ICluster] <- nodes;
     //                                            node : ICluster <- nodeFut)
    //    yield ListenableFutureObservable.to(node.gossip().listen())
      val futureOfList = Future.sequence(listening)
      timer1.timed(() => nodes(0).map{node => node.gossip().spread(new Message()); futureOfList})
      futureOfList onComplete(_ => nodes.foreach(_.foreach(_.leave())))
    }
  }


  // timer1.timed()


}
