package com.fotolog.redis

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

  "A publish" should "return Int result" in {
    publisher.publish[String]("test" , "Hello") shouldEqual 0
    subscriber.subscribe[String]("test"){(channel, msg) => None }
    publisher.publish[String]("test" , "Hi subscriber") shouldEqual 1
  }

  "A subscribe/unsubscribe" should "return String result" in {
    var channelRes = "not used"
    var msgRes = "not used"

    subscriber.subscribe[String]("test", "test1"){(channel, msg) =>
      channelRes = channel
      msgRes = msg
    }
    publisher.publish[String]("test", "Hello")
    channelRes shouldEqual "test"
    msgRes shouldEqual "Hello"

    publisher1.publish[String]("test1", "World")
    channelRes shouldEqual "test1"
    msgRes shouldEqual "World"

    subscriber.unsubscribe("test")
    publisher.publish[String]("test", "Hello") shouldEqual 0
    publisher1.publish[String]("test1", "1pub msg")
    channelRes shouldEqual "test1"
    msgRes shouldEqual "1pub msg"
  }


}
