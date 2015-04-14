package com.fotolog.redis

import org.junit.Assert._
import org.junit.{Test, After, Before}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Created by sergeykhruschak on 4/9/15.
 */
class PubSubTest {
  val c = RedisClient("localhost", 6379)

  // @Before def setUp() { c.flushall }
  // @After def tearDown() { c.flushall }

  //@Test def testPublish() {
  //  c.publish[String]("test", "message-test")
  //}

  @Test  def testSubscribe() {
    val subscriptionRes= c.subscribe[String]("baz", "bar", "gee") { (channel, msg) =>
      println("Subscriber1: Got data from channel " + channel + ":" + msg)
    }

    assertEquals(Seq(1, 2, 3), subscriptionRes)

    c.subscribe[String]("baz", "tee") { (channel, msg) =>
      println("Subscriber2: Got data from channel " + channel + ":" + msg)
    }

    Thread.sleep(690000L)

    assertEquals(Seq(2, 1, 1), c.unsubscribe("baz", "gee", "fee"))

    // c.set("key", "test")
  }
}
