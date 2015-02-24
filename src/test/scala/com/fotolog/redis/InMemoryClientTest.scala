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
    assertTrue(c.set("foo", "bar", 1))
    assertTrue(c.exists("foo"))

    assertEquals("bar", c.get[String]("foo").get)
    assertEquals(KeyType.String, c.keytype("foo"))

    assertEquals("One key has to be deleted", 1, c.del("foo"))
    assertFalse("Key should not exist", c.exists("foo"))

    assertEquals("No keys has to be deleted", 0, c.del("foo"))
    //assertEquals(-2, c.ttl("foo"))
  }
}
