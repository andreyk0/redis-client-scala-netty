package com.fotolog.redis

/**
  * Created by sergeykhruschak on 6/20/16.
  */
trait TestClient {
  def createClient: RedisClient =
    RedisClient(sys.env.getOrElse("TEST_DB_HOST", "localhost"), password = sys.env.get("TEST_DB_PASS"))
}
