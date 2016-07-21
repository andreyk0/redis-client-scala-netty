package com.fotolog.redis

import org.scalatest._
/**
 *
 * @author Sergey Khruschak <sergey.khruschak@gmail.com>
 *         Created on 5/13/15.
 */
class HyperLogLogSpec extends FlatSpec with Matchers with TestClient {

  "A HyperLogLog" should "count values properly" in {
    client.pfcount("hll") shouldEqual 0
    client.pfadd("hll", "a", "b", "c", "d") shouldBe true

    client.pfcount("hll") shouldEqual 4

    client.pfadd("hll", "a", "d", "e") shouldBe true
    client.pfcount("hll") shouldEqual 5
  }

  it should "be able to merge values" in {
    client.pfadd("h1", "a", "b", "c", "d") shouldBe true
    client.pfadd("h2", "a", "b", "f", "g") shouldBe true
    client.pfmerge("h3", "h1", "h2") shouldBe true

    client.pfcount("h3") shouldEqual 6
  }

}
