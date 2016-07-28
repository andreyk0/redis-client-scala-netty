package com.fotolog.redis

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 17.07.16.
  */
class PubSubCommandsSpec extends FlatSpec with Matchers with TestClient {

  val publisher, publisher1 = createClient
  val subscriber = createClient

  "A publish" should "return a number of clients that received the message" in {
    publisher.flushall
    publisher.publish[String]("test" , "Hello") shouldEqual 0
    subscriber.subscribe[String]("test"){(channel, msg) => None }
    publisher.publish[String]("test" , "Hi subscriber") shouldEqual 1
  }

  "A subscribe/unsubscribe" should "publish and receive messages" in {
    val latch = new CountDownLatch(1)
    var receivedMessage = false
    var messageData = "not used"

    val subscriptionRes = client.subscribe[String]("baz", "test") { (channel, msg) =>
      messageData = msg
      receivedMessage = true
      latch.countDown()
    }

    subscriptionRes shouldEqual Seq(1, 2)

    var receivedMessage2 = false

    client.subscribe[String]("b*", "tezz") { (channel, msg) =>
      receivedMessage2 = true
    }

    publishMsg("test", "message")

    latch.await(5, TimeUnit.SECONDS)
    Thread.sleep(300) // wait for second response

    receivedMessage shouldEqual true
    receivedMessage2 shouldEqual false
    messageData shouldEqual "message"
  }

  "A subscribe" should "allow to subscribe on a pattern" in {
    val latch = new CountDownLatch(1)
    var receivedMessage = false

    val subscriptionRes = createClient().subscribe[String]("glob*") { (channel, msg) =>
      receivedMessage = true
      latch.countDown()
    }

    subscriptionRes shouldEqual Seq(1)

    publishMsg("global", "message")

    latch.await(5, TimeUnit.SECONDS)

    receivedMessage shouldEqual true
  }

  "Command" should "fail after subscribe" in {
    val client = createClient()
    client.subscribe[String]("channel") { (c, m) => }

    intercept[RedisException] {
      client.incr("baz")
    }

  }

  /* TODO: @Test def testSubscribeUnsubscribe() {
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
  } */

  private def publishMsg(channel: String, msg: String) { createClient().publish(channel, msg) }


}
