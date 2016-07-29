package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

class GenericCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A ping" should "just work" in {
    client.ping shouldBe true
  }

  "An exists" should "check if key exists" in {
    client.exists("foo") shouldBe false
    client.set("foo", "bar", 1) shouldBe true
    client.exists("foo") shouldBe true
    client.del("foo") shouldEqual 1
    client.exists("foo") shouldBe false
    client.del("foo") shouldEqual 0
    client.ttl("foo") shouldEqual -2
  }

  "An expire" should "expire keys" in {
    client.set("foo", "bar") shouldBe true
    client.expire("foo", 100) shouldBe true
    client.ttl("foo") shouldEqual 100
    client.persist("foo") shouldBe true
    client.ttl("foo") shouldEqual -1
    client.ttl("somekey") shouldEqual -2
  }

  "Keys" should "return all matched keys" in {
    client.set("prefix:1" -> 1, "prefix:2" -> 2)
    client.keys("prefix:*") shouldEqual Set("prefix:1", "prefix:2")
    client.del("prefix:1")
    client.del("prefix:2")
  }

  "A transaction" should "allow to set values" in {
    client.withTransaction { cli =>
      cli.setAsync("tx_key", "tx_val")
    }
  }

  "Scripting" should "accept and execute scripts" in {
    import com.fotolog.redis.primitives.Redlock._

    client.eval[Int]("return ARGV[1];", ("anyKey", "2")) shouldEqual Set(2)
    client.scriptLoad("return ARGV[1];") shouldEqual "4629ab89363d08ca29abd4bb0aaf5ed70e2bb228"
    client.evalsha[Int]("4629ab89363d08ca29abd4bb0aaf5ed70e2bb228", ("key", "4")) shouldEqual Set(4)

    client.setNx("lock_key", "lock_value") shouldEqual true
    client.eval[Int](UNLOCK_SCRIPT, ("lock_key", "no_lock_value")) shouldEqual Set(0)
    client.eval[Int](UNLOCK_SCRIPT, ("lock_key", "lock_value")) shouldEqual Set(1)

    client.scriptExists("4629ab89363d08ca29abd4bb0aaf5ed70e2bb228") shouldBe true
    client.scriptFlush() shouldBe true

    client.scriptExists("4629ab89363d08ca29abd4bb0aaf5ed70e2bb228") shouldBe false
  }

  /*
  @Test def testMgetMset() {
    assertTrue(c.set("foo" -> "foo1", "bar" -> "bar1", "baz" -> "baz1"))
    assertEquals(Seq(Some("foo1"), None, Some("bar1"), Some("baz1")), c.get[String]("foo", "blah", "bar", "baz"))
    assertEquals(Map("foo" -> "foo1", "bar" -> "bar1", "baz" -> "baz1"), c.mget[String]("foo", "blah", "baz", "bar"))
    assertEquals(Map("foo" -> "foo1", "bar" -> "bar1", "baz" -> "baz1"), c.mget[String]("foo", "blah", "baz", "nothing", "bar"))

    assertTrue(c.set("foobar" -> "foo2"))
    assertEquals(Some("foo2"), c.get[String]("foobar"))
  }

  @Test def testSetNxEx() {
    assertTrue(c.setNx("blah", "blah"))
    assertFalse(c.setNx("blah", "boo"))

    assertTrue(c.setNx("foobar" -> "foo2"))

    assertTrue( c.setNx("foo" -> "foo1", "bar" -> "bar1", "baz" -> "baz1") )

    assertFalse(c.setNx("xxx" -> "yyy", "bar" -> "blah"))

    assertTrue(c.setNx("three", "three", 100))
    assertFalse(c.setNx("three", "three", 10))
    assertFalse(c.setNx("blah", "blah", 10))
  }

  @Test def testGetSet() {
    assertEquals(None, c.getset("foo", "bar"))
    assertEquals(Some("bar"), c.getset[String]("foo", "baz"))
    assertEquals(Some("baz"), c.getset[String]("foo", "blah"))
  }

  @Test def testSetEx() {
    assertTrue(c.set("foo", "bar", 123))
    assertEquals(Some("bar"), c.get[String]("foo"))
  }

  @Test def testIncrDecr() {
    assertEquals(1, c.incr("foo"))
    assertEquals(3, c.incr("foo", 2))
    assertEquals(2, c.decr("foo"))
    assertEquals(-1, c.decr("foo", 3))
  }

  @Test def testAppend() {
    assertTrue(c.set("foo", "bar"))
    assertEquals(6, c.append("foo", "baz"))
    assertEquals(Some("barbaz"), c.get[String]("foo"))
  }

  @Test def testSubstr() {
    assertEquals(None, c.substr[String]("foo", 0, 1))
    assertTrue(c.set("foo", "bar"))
    assertEquals(Some("ba"), c.substr[String]("foo", 0, 1))
  }



  @Test def testLists() {
    assertEquals(Seq(), c.lrange[String]("foo", 0, 100))
    assertEquals(1, c.rpush("foo", "ccc"))
    assertEquals(2, c.lpush("foo", "bbb"))
    assertEquals(3, c.rpush("foo", "ddd"))
    assertEquals(4, c.lpush("foo", "aaa"))
    assertEquals(4, c.llen("foo"))
    assertEquals(Seq("aaa", "bbb", "ccc", "ddd"), c.lrange[String]("foo", 0, 100))
    assertEquals(Seq("bbb", "ccc"), c.lrange[String]("foo", 1, 2))
    assertTrue(c.ltrim("foo", 0, 2))
    assertEquals(Seq("aaa", "bbb", "ccc"), c.lrange[String]("foo", 0, 100))
    assertEquals(Some("bbb"), c.lindex[String]("foo", 1))
    assertEquals(None, c.lindex[String]("foo", 100))
    assertTrue(c.lset("foo", 1, "BBB"))
    assertEquals(Seq("aaa", "BBB", "ccc"), c.lrange[String]("foo", 0, 100))
    assertEquals(1, c.lrem[String]("foo", 10, "BBB"))
    assertEquals(Seq("aaa", "ccc"), c.lrange[String]("foo", 0, 100))
    assertEquals(Some("ccc"), c.rpop[String]("foo"))
    assertEquals(Seq("aaa"), c.lrange[String]("foo", 0, 100))
    assertEquals(2, c.rpush("foo", "bbb"))
    assertEquals(Some("aaa"), c.lpop[String]("foo"))
    assertEquals(Some("bbb"), c.lpop[String]("foo"))
    assertEquals(None, c.lpop[String]("foo"))
    assertEquals(None, c.rpop[String]("foo"))

    assertEquals(1, c.rpush("foo", "aaa"))
    assertEquals(Some("aaa"), c.rpoplpush[String]("foo", "foo1"))
    assertEquals(Seq("aaa"), c.lrange[String]("foo1", 0, 100))
  }

  @Test def testNumericConversions() {
    testIntVals.foreach{ i=>
      assertTrue(c.set("foo", i))
      assertEquals(Some(i), c.get[Int]("foo"))
    }

    testLongVals.foreach{ i=>
      assertTrue(c.set("foo", i))
      assertEquals(Some(i), c.get[Long]("foo"))
    }
  }

*/
  private def testIntVals = 0 :: {for(i<-0 to 30) yield List(1<<i,-(1<<i))}.toList.flatten
  private def testLongVals = 0l :: {for(i<-0 to 62) yield List(1l<<i,-(1l<<i))}.toList.flatten


}
