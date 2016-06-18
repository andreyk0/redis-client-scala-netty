package com.fotolog.redis

import com.fotolog.redis.primitives.{SuccessfulLock, Redlock}
import com.googlecode.junittoolbox.ParallelRunner

import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Test, After, Before}

@RunWith(classOf[ParallelRunner])
class MultiRedlockTest {
/* TODO: setup multi redis environment for testing
  val clients = Array(
    RedisClient("localhost", 6379),
    RedisClient("localhost", 6378),
    RedisClient("localhost", 6376)
  )

  val l = Redlock(clients)

  @Before def setUp() {
    clients.foreach(_.flushall)
  }

  @Test def testDistlockSuccess() {

    val lock = l.lock("redlock:key", 60*60, 5).asInstanceOf[SuccessfulLock]

    assertTrue("Should lock redis servers", lock.successful)
    assertEquals("Key should equals", "redlock:key", lock.key)
    assertTrue("Key should exist", clients(0).exists(lock.key))
  }

  @Test def testDistlockFailed() {
    Thread.sleep(100)
    val lock = l.lock("redlock:key", 60*60, 5)

    assertFalse("Should not lock redis servers", lock.successful)
  }
*/
}
