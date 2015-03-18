package com.fotolog.redis

import com.fotolog.redis.primitives.{SuccessfulLock, Redlock}
import org.junit.Assert._
import org.junit.{Test, After, Before}

class SingleRedlockTest {

  val c = RedisClient()
  val l = Redlock(c)

  @Before def setUp() { c.flushall }

  @After def tearDown() {
    c.flushall
    c.shutdown()
  }

  @Test def testDistlock(): Unit = {
    
    val key = "redlock:key"
    val ttl = 60*60
    val tries = 5

    val lock = l.lock(key, ttl, tries)
    assertTrue("Should lock single redis server", lock.successful)

    assertFalse("Should not lock single redis server with exist locks", l.lock(key, ttl, tries).successful)

    l.unlock(lock)

    assertTrue("Should allow to lock after unlock", l.lock(key, ttl, tries).successful)

  }
}
