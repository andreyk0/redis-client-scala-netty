package com.fotolog.redis

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
  * Created by sergeykhruschak on 6/20/16.
  */
trait TestClient extends BeforeAndAfterEach with BeforeAndAfterAll { this: Suite =>

  val client: RedisClient = createClient()

  override def beforeEach() {
    super.beforeEach()
    client.flushall
  }

  override def afterAll() = {
    client.shutdown()
  }

  def createClient() = RedisClient(sys.env.getOrElse("TEST_DB_HOST", "localhost"), password = sys.env.get("TEST_DB_PASS"))

}
