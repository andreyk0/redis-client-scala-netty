package com.fotolog.redis

import org.scalameter.api._
import org.scalameter.reporting.RegressionReporter


import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ClientBenchmark extends Bench.LocalTime {

  private var client: RedisClient = _

  /* configuration */

  override def aggregator = Aggregator.average

  override def reporter = Reporter.Composite(
    new RegressionReporter(
      RegressionReporter.Tester.Accepter(),
      RegressionReporter.Historian.Complete()
    ),
    HtmlReporter(true)
  )

  override def measurer = new Measurer.Default

  override type SameType = this.type

  /* inputs */

  val sizes = Gen.range("size")(10000, 30000, 10000)

  /* tests */

  performance of "Client" in {
    measure method "PING" in {
      using(sizes) config {
        exec.maxWarmupRuns -> 3
        exec.benchRuns -> 3
        exec.independentSamples -> 3
      } setUp { _ =>
        client = RedisClient()
      } tearDown { _ =>
        client = null
      } in { i =>
        val future = Future.traverse(1 to i) { i =>
          client.pingAsync
        }
        Await.result(future, 30 seconds)
      }
    }

    measure method "GET" in {
      using(sizes) config {
        exec.maxWarmupRuns -> 3
        exec.benchRuns -> 3
        exec.independentSamples -> 3
      } setUp { _ =>
        client = RedisClient()
        client.set("foo", "bar")
      } tearDown { _ =>
        client.del("foo")
        client = null
      } in { i =>
        val future = Future.traverse(1 to i) { i =>
          client.getAsync[String]("foo")
        }
        Await.result(future, 30 seconds)
      }
    }

    measure method "SET" in {
      using(sizes) config {
        exec.maxWarmupRuns -> 3
        exec.benchRuns -> 3
        exec.independentSamples -> 3
      } setUp { _ =>
        client = RedisClient()
      } tearDown { _ =>
        client.del("foo")
        client = null
      } in { i =>
        val future = Future.traverse(1 to i) { i =>
          client.setAsync("foo", "bar")
        }
        Await.result(future, 30 seconds)
      }
    }
  }

}