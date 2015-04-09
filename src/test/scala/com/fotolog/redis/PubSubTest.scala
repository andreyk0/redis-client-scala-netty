package com.fotolog.redis

import org.junit.Assert._
import org.junit.{Test, After, Before}

/**
 * Created by sergeykhruschak on 4/9/15.
 */
class PubSubTest {
  val c = RedisClient("localhost", 6379)

  // @Before def setUp() { c.flushall }
  // @After def tearDown() { c.flushall }

  @Test def testPublish() {
    c.publish[String]("baz", "message-test")
  }

  @Test  def testSubscribe() {
    c.subscribe[String]("baz", "foo") { (x, y) =>
      println("Got data from channels: " + x + ":" + y)
    }
  }
}
