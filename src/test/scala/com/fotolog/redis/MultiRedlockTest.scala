package com.fotolog.redis

import com.fotolog.redis.primitives.{SuccessfulLock, Redlock}
import com.googlecode.junittoolbox.ParallelRunner

import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Test, After, Before}

@RunWith(classOf[ParallelRunner])
class MultiRedlockTest {

  val c0 = RedisClient("localhost", 6379)
  val c1 = RedisClient("localhost", 6378)
  val c2 = RedisClient("localhost", 6376)

  val l = Redlock(c0, c1, c2)

  @Before def setUp() {
    c0.flushall
    c1.flushall
    c2.flushall
  }

  @Test def testDistlockSuccess(): Unit = {

    val lock = l.lock("redlock:key", 60*60, 5).asInstanceOf[SuccessfulLock]

    assertEquals("Should lock redis servers", true, lock.successful)
    assertEquals("Key should equals", "redlock:key", lock.key)
    assertEquals("Key should exist", true, c0.exists(lock.key))

  }

  @Test def testDistlockFailed(): Unit = {

    Thread.sleep(100)
    val lock = l.lock("redlock:key", 60*60, 5)

    assertEquals("Should not lock redis servers", false, lock.successful)

  }

}
