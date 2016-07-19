package com.fotolog.redis

import com.fotolog.redis.primitives.Redlock
import org.scalatest.{FlatSpec, Matchers}

class SingleRedlockSpec extends FlatSpec with Matchers with TestClient {

  val c = createClient
  val l = Redlock(c)
/* TODO:
  @Before def setUp() { c.flushall }

  @Test def testDistlock(): Unit = {

    val key = "redlock:key"
    val ttl = 60*60
    val tries = 5

    val lock = l.lock(key, ttl, tries)

    assertTrue("Should lock single redis server", lock.successful)
    assertFalse("Should not lock single redis server with existing lock", l.lock(key, ttl, tries).successful)

    l.unlock(lock)

    assertTrue("Should allow to lock after unlock", l.lock(key, ttl, tries).successful)
  }


  @Test def testWithLock(): Unit = {

    val visits = Array(false, false, false)

    val lockStatuses = for(i <- 0 to 2) yield {
      l.withLock("resource-name") {
        visits(i) = true
      }
    }

    val expected = Seq(true, false, false)

    assertEquals("Code should be executed only for the first time", expected, visits.toSeq)
    assertEquals("First lock attempt should succeed, other - fail", expected, lockStatuses.toSeq)
  }
  */
}
