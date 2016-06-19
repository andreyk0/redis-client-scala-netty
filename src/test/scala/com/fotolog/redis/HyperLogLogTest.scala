package com.fotolog.redis

import org.junit.Assert._
import org.junit.{Test, After, Before}

/**
 *
 * @author Sergey Khruschak <sergey.khruschak@gmail.com>
 *         Created on 5/13/15.
 */
class HyperLogLogTest extends TestClient {
  val c = createClient

  @Before def setUp() { c.flushall }
  @After def tearDown() { c.flushall }

  @Test def testPfAddAndCount() {
    assertEquals(0, c.pfcount("hll"))

    assertTrue(c.pfadd("hll", "a", "b", "c", "d"))
    assertEquals(4, c.pfcount("hll"))

    assertTrue(c.pfadd("hll", "a", "d", "e"))
    assertEquals(5, c.pfcount("hll"))
  }

  @Test def testPfMerge() {
    assertTrue(c.pfadd("h1", "a", "b", "c", "d"))
    assertTrue(c.pfadd("h2", "a", "b", "f", "g"))
    assertTrue(c.pfmerge("h3", "h1", "h2"))

    assertEquals(6, c.pfcount("h3"))
  }
}
