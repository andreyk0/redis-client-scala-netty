package com.fotolog.redis

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 17.07.16.
  */
class PubSubCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A publish" should "return a number of clients that received the message" in {
    val publisher = createClient
    val subscriber = createClient

    publisher.publish[String]("test" , "Hello") shouldEqual 0
    subscriber.subscribe[String]("test"){(channel, msg) => None }
    publisher.publish[String]("test" , "Hi subscriber") shouldEqual 1

    publisher.shutdown()
    subscriber.shutdown()
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

    receivedMessage shouldEqual true
    receivedMessage2 shouldEqual false
    messageData shouldEqual "message"
  }

  "A subscribe" should "allow to subscribe on a pattern" in {
    val latch = new CountDownLatch(1)
    var receivedMessage = false

    val subscriptionRes = createClient.subscribe[String]("glob*") { (channel, msg) =>
      receivedMessage = true
      latch.countDown()
    }

    subscriptionRes shouldEqual Seq(1)

    publishMsg("global", "message")

    latch.await(15, TimeUnit.SECONDS)

    receivedMessage shouldEqual true
  }

  "A Subscribe" should "should disable any command" in {
    val client = createClient
    client.subscribe[String]("channel") { (c, m) => }

    intercept[RedisException] {
      client.incr("baz")
    }

  }

  "Unsubscribe" should "fail after subscribe" in {
    val client = createClient
    val subscriptionRes = client.subscribe[String]("baz", "bar", "gee") { (channel, msg) =>
      println("Subscriber1: Got data from channel " + channel + ":" + msg)
    }

    subscriptionRes shouldEqual Seq(1, 2, 3)

    client.subscribe[String]("baz", "tee") { (channel, msg) =>
      println("Subscriber2: Got data from channel " + channel + ":" + msg)
    }

    client.unsubscribe("baz", "gee", "fee", "tee", "bar") shouldEqual Seq(3, 2, 2, 1, 0)

    // we unsubscribed from all channels so now have to be able to send any command
    client.set("key", "test") shouldBe true
    client.get[String]("key") shouldEqual Some("test")
  }

  "A unsubscribe all" should "work correctly" in {
    val publisher = createClient
    val subscriber = createClient
    val latch = new CountDownLatch(1)

    var msgsRes = "not used"

    subscriber.subscribe[String]("test", "test1") { (channel, msg) =>
      msgsRes = msg
      latch.countDown()
    }

    publisher.publish[String]("test", "Hello")

    latch.await(15, TimeUnit.SECONDS)

    msgsRes shouldEqual "Hello"

    subscriber.unsubscribe
  }

  private def publishMsg(channel: String, msg: String) { createClient.publish(channel, msg) }
}
