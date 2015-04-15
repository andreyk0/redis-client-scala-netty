package com.fotolog.redis

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.junit.Assert._
import org.junit.{Before, Test}

/**
 * Tests for publish/subscribe mechanism.
 *
 * @author Sergey Khruschak on 4/9/15.
 */
class PubSubTest {
  var c: RedisClient = _

  @Before def setUp() {
    c = RedisClient()
    c.flushall
  }

  @Test def testPublish() {
    c.publish[String]("test", "message-test")
  }

  @Test def testSubscribe() {
    val latch = new CountDownLatch(1)
    var receivedMessage = false

    val subscriptionRes = c.subscribe[String]("baz", "test") { (channel, msg) =>
      assertEquals("test", channel)
      assertEquals("message", msg)
      receivedMessage = true
      latch.countDown()
    }

    assertEquals(Seq(1, 2), subscriptionRes)

    var receivedMessage2 = false
    c.subscribe[String]("b*", "tezz") { (channel, msg) =>
      receivedMessage2 = true
    }

    publishMsg("test", "message")

    latch.await(5, TimeUnit.SECONDS)
    Thread.sleep(200)

    assertTrue(receivedMessage)
    assertFalse(receivedMessage2)
  }


  @Test(expected = classOf[RedisException])
  def testAnyCommandAfterSubscribe() {
    c.subscribe[String]("channel") { (c, m) => }
    c.setNx("someData", "data")
  }

  @Test def testSubscribeUnsubscribe() {
    val subscriptionRes = c.subscribe[String]("baz", "bar", "gee") { (channel, msg) =>
      println("Subscriber1: Got data from channel " + channel + ":" + msg)
    }

    assertEquals(Seq(1, 2, 3), subscriptionRes)

    c.subscribe[String]("baz", "tee") { (channel, msg) =>
      println("Subscriber2: Got data from channel " + channel + ":" + msg)
    }

    assertEquals(Seq(3, 2, 2, 1, 0), c.unsubscribe("baz", "gee", "fee", "tee", "bar"))

    // we unsubscribed from all channels so now have to be able to send any command
    assertTrue(c.set("key", "test"))
    assertEquals("test", c.get[String]("key").get)
  }


  private def publishMsg(channel: String, msg: String) {
    assertEquals(2, RedisClient().publish(channel, msg))
  }

}
