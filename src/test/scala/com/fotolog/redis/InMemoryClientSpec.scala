package com.fotolog.redis

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.{FlatSpec, Matchers}

/**
 * Will be merged with RedisClientTest when in-memory client will support
 * full set of operations.
 */
class InMemoryClientSpec extends FlatSpec with Matchers {
  val c = RedisClient("mem:test")

  "A In Memory Client" should "respond to ping" in {
    c.ping() shouldBe true
  }

  it should "support get, set, exists and type operations" in {
    c.exists("foo") shouldBe false
    c.set("foo", "bar", 2592000) shouldBe true

    c.exists("foo") shouldBe true

    c.get[String]("foo").get shouldEqual "bar"
    c.keytype("foo") shouldEqual KeyType.String

    // keys test
    c.set("boo", "baz") shouldBe true
    c.set("baz", "bar") shouldBe true
    c.set("buzzword", "bar") shouldBe true

    c.keys("?oo") shouldEqual Set("foo", "boo")
    c.keys("*") shouldEqual Set("foo", "boo", "baz", "buzzword")
    c.keys("???") shouldEqual Set("foo", "boo", "baz")
    c.keys("*b*") shouldEqual Set("baz", "buzzword", "boo")

    c.del("foo") shouldEqual 1
    c.exists("foo") shouldBe false

    c.del("foo") shouldEqual 0

    // rename
    c.rename("baz", "rbaz") shouldBe true
    c.exists("baz") shouldBe false
    c.get[String]("rbaz") shouldEqual Some("bar")

    // rename nx
    c.rename("rbaz", "buzzword", true) shouldBe false
  }

  it should "support inc/dec operations" in {
    c.set("foo", 1) shouldBe true
    c.incr("foo", 10) shouldEqual 11
    c.incr("foo", -11) shouldEqual 0
    c.incr("unexistent", -5) shouldEqual -5
  }

  it should "fail to rename inexistent key" in {
    intercept[RedisException] {
      c.rename("non-existent", "newkey")
    }
  }

  it should "fail to increment inexistent key" in {
    c.set("baz", "bar") shouldBe true

    intercept[RedisException] {
      c.incr("baz")
    }

  }

  /*
  @Test def testKeyTtl() {
    assertTrue(c.set("foo", "bar", 5))
    assertTrue(c.ttl("foo") <= 5)

    assertTrue(c.set("baz", "foo"))

    assertEquals("Ttl if not set should equal -1", -1, c.ttl("baz"))

    assertEquals("Ttl of nonexistent entity has to be -2", -2, c.ttl("bar"))

    assertTrue(c.set("bee", "test", 100))
    assertTrue(c.persist("bee"))
    assertEquals("Ttl of persisted should equal -1", -1, c.ttl("bee"))
  }

  @Test def testHash() {
    assertTrue("Problem with creating hash", c.hmset("foo", "one" -> "another"))
    assertTrue("Problem with creating 2 values hash", c.hmset("bar", "baz1" -> "1", "baz2" -> "2"))

    assertEquals("Hash value is wrong", Some("another"), c.hget[String]("foo", "one"))
    assertEquals("Hash value is wrong", Some("1"), c.hget[String]("bar", "baz1"))
    assertEquals("Resulting map with 2 values", Map("baz1" -> "1", "baz2" -> "2"), c.hmget[String]("bar", "baz1", "baz2"))
    assertEquals("Resulting map with 1 values", Map("baz2" -> "2"), c.hmget[String]("bar", "baz2"))

    assertEquals("Was 2 plus 5 has to give 7", 7, c.hincr("bar", "baz2", 5))
    assertEquals("Was 1 minus 4 has to give -3", -3, c.hincr("bar", "baz1", -4))

    assertEquals("Changed map has to have values 7, -3", Map("baz1" -> "-3", "baz2" -> "7"), c.hmget[String]("bar", "baz1", "baz2"))

    assertTrue(c.hmset[String]("zoo-key", "foo" -> "{foo}", "baz" -> "{baz}", "vaz" -> "{vaz}", "bzr" -> "{bzr}", "wry" -> "{wry}"))

    val map = c.hmget[String]("zoo-key", "foo", "bzr", "vaz", "wry")

    for(k <- map.keys) {
      assertEquals("Values don't correspond to keys in result", "{" + k + "}", map(k))
    }

    assertEquals(Map("vaz" -> "{vaz}", "bzr" -> "{bzr}", "wry" -> "{wry}"), c.hmget[String]("zoo-key", "boo", "bzr", "vaz", "wry"))
  }

  @Test def testSet() {
    assertEquals("Should add 2 elements and create set", 2, c.sadd("sport", "tennis", "hockey"))
    assertEquals("Should add only one element", 1, c.sadd("sport", "football"))
    assertEquals("Should not add any elements", 0, c.sadd("sport", "hockey"))

    assertTrue("Elements should be in set", c.sismember("sport", "hockey"))
    assertFalse("Elements should not be in set", c.sismember("sport", "ski"))
    assertFalse("No set – no elements", c.sismember("drink", "ski"))

    assertEquals("Resulting set has to contain all elements", Set("tennis", "hockey", "football"), c.smembers[String]("sport"))
  }

  @Test def testRedlockScript() {
    import com.fotolog.redis.primitives.Redlock._

    assertTrue(c.set("redlock:key", "redlock:value"))
    assertEquals("Should not unlock redis server with nonexistent value", Set(0), c.eval[Int](UNLOCK_SCRIPT, "redlock:key" -> "non:value"))
    assertEquals("Should unlock redis server", Set(1), c.eval[Int](UNLOCK_SCRIPT, "redlock:key" -> "redlock:value"))

  }

  @Test def testPubSub() {
    val latch = new CountDownLatch(1)
    var invoked = false

    val subscribtionRes = RedisClient("mem:test").subscribe[String]("f*", "foo", "f*", "bar") { (channel, msg) =>
      invoked = true
      latch.countDown()
    }

    assertEquals(Seq(1, 2, 2, 3), subscribtionRes)

    c.publish("fee", "message")

    latch.await(5, TimeUnit.SECONDS)

    assertTrue(invoked)

  }
  */
}
