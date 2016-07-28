package com.fotolog.redis

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 17.07.16.
  */
class PubSubCommandsSpec extends FlatSpec with Matchers with TestClient {

  val publisher, publisher1 = createClient
  val subscriber = createClient

  publisher.flushall
  publisher1.flushall
  subscriber.flushall

  "A publish" should "return a number of clients that received the message" in {
    publisher.publish[String]("test" , "Hello") shouldEqual 0
    subscriber.subscribe[String]("test"){(channel, msg) => None }
    publisher.publish[String]("test" , "Hi subscriber") shouldEqual 1
  }

  "A subscribe/unsubscribe" should "return String result" in {
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

  private def publishMsg(channel: String, msg: String) { createClient().publish(channel, msg) }


}
