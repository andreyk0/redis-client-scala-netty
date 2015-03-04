package com.fotolog.redis

import junit.framework.TestCase
import org.junit.Assert._

class InMemoryClientTest extends TestCase {
  val c = RedisClient("mem:test")

  override def setUp() = c.flushall
  override def tearDown() = c.flushall

  def testConnectionCommands(): Unit = {
    assertTrue(c.ping())
  }

  def testPingGetSetExistsType() {
    assertFalse(c.exists("foo"))
    assertTrue(c.set("foo", "bar", 2592000))

    assertTrue(c.exists("foo"))

    assertEquals("bar", c.get[String]("foo").get)
    assertEquals(KeyType.String, c.keytype("foo"))

    assertEquals("One key has to be deleted", 1, c.del("foo"))
    assertFalse("Key should not exist", c.exists("foo"))

    assertEquals("No keys has to be deleted", 0, c.del("foo"))
  }

  def testKeyTtl() {
    assertTrue(c.set("foo", "bar", 5))
    assertTrue(c.ttl("foo") <= 5)

    assertTrue(c.set("baz", "foo"))

    assertEquals("Ttl if not set should equal -1", -1, c.ttl("baz"))

    assertEquals("Ttl of nonexistent entity has to be -2", -2, c.ttl("bar"))
  }

  def testSet() {
    assertEquals("Should return count of new elements - 2", 2, c.sadd("sport", "tennis", "hockey"))
    assertEquals("Should return count of new elements - 1", 1, c.sadd("sport", "football"))
    assertEquals("Should return count of new elements - 0", 0, c.sadd("sport", "hockey"))

    assertEquals("Elements should exist", true, c.sismember("sport", "hockey"))
    assertEquals("Elements should not exist", false, c.sismember("sport", "ski"))
  }
}
