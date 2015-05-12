package com.fotolog.redis

import org.junit.Assert._
import org.junit.{After, Before, Test}

/**
 * Will be merged with RedisClientTest when in-memory client will support
 * full set of operations.
 */
class InMemoryClientTest {
  val c = RedisClient("mem:test")

  @Before def setUp() { c.flushall }
  @After def tearDown() { c.flushall }

  @Test def testConnectionCommands() {
    assertTrue(c.ping())
  }

  @Test def testGetSetExistsType() {
    assertFalse(c.exists("foo"))
    assertTrue(c.set("foo", "bar", 2592000))

    assertTrue(c.exists("foo"))

    assertEquals("bar", c.get[String]("foo").get)
    assertEquals(KeyType.String, c.keytype("foo"))

    // keys test
    assertTrue(c.set("boo", "baz"))
    assertTrue(c.set("baz", "bar"))
    assertTrue(c.set("buzzword", "bar"))

    assertEquals(Set("foo", "boo"), c.keys("?oo"))
    assertEquals(Set("foo", "boo", "baz", "buzzword"), c.keys("*"))
    assertEquals(Set("foo", "boo", "baz"), c.keys("???"))
    assertEquals(Set("baz", "buzzword", "boo"), c.keys("*b*"))

    assertEquals("One key has to be deleted", 1, c.del("foo"))
    assertFalse("Key should not exist", c.exists("foo"))

    assertEquals("No keys has to be deleted", 0, c.del("foo"))

    // rename
    assertTrue("Rename should succeed", c.rename("baz", "rbaz"))
    assertFalse(c.exists("baz"))
    assertEquals(Some("bar"), c.get[String]("rbaz"))

    // rename nx
    assertFalse("Rename to existent key should do nothing", c.rename("rbaz", "buzzword", true))
  }

  @Test def testStringCommands(): Unit = {
    assertTrue(c.set("foo", 1))
    assertEquals(11, c.incr("foo", 10))
    assertEquals(0, c.incr("foo", -11))
    assertEquals(-5, c.incr("unexistent", -5))
  }

  @Test(expected = classOf[RedisException])
  def testRenameFailure() {
    c.rename("non-existent", "newkey")
  }

  @Test(expected = classOf[RedisException])
  def testIncrementFailure() {
    assertTrue(c.set("baz", "bar"))
    c.incr("baz")
  }

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

  @Test def testScript() {
    import com.fotolog.redis.primitives.Redlock._

    assertTrue(c.set("redlock:key", "redlock:value"))
    assertEquals("Should not unlock redis server with nonexistent value", Set(0), c.eval[Int](UNLOCK_SCRIPT, "redlock:key" -> "non:value"))
    assertEquals("Should unlock redis server", Set(1), c.eval[Int](UNLOCK_SCRIPT, "redlock:key" -> "redlock:value"))

  }
}
