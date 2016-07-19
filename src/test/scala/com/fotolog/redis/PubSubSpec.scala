package com.fotolog.redis

import org.scalatest.{FlatSpec, Matchers}

/**
 * Tests for publish/subscribe mechanism.
 *
 * @author Sergey Khruschak on 4/9/15.
 */
class PubSubSpec extends FlatSpec with Matchers with TestClient {
  var c: RedisClient = _
/*
  @Before def setUp() {
    c = createClient
    c.flushall
  }

  @Test def testPublish() {
    c.publish[String]("test", "message-test")
  }

  @Test def testSubscribe() {
    val latch = new CountDownLatch(1)
    var receivedMessage = false
    var messageData = "not used"

    val subscriptionRes = c.subscribe[String]("baz", "test") { (channel, msg) =>
      messageData = msg
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
    Thread.sleep(300) // wait for second response

    assertTrue(receivedMessage)
    assertFalse(receivedMessage2)
    assertEquals("message", messageData)
  }


  @Test def testPsubscribe() {
    val latch = new CountDownLatch(1)
    var receivedMessage = false

    val subscriptionRes = c.subscribe[String]("glob*") { (channel, msg) =>
      receivedMessage = true
      latch.countDown()
    }

    assertEquals(Seq(1), subscriptionRes)

    publishMsg("global", "message")

    latch.await(5, TimeUnit.SECONDS)

    assertTrue(receivedMessage)
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
    assertEquals(1, createClient.publish(channel, msg))
  }

*/
}
