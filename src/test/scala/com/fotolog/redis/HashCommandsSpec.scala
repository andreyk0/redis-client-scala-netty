package com.fotolog.redis

/**
  * Created by faiaz on 12.07.16.
  */
import org.scalatest.{FlatSpec, Matchers}

class HashCommandsSpec extends FlatSpec with Matchers with TestClient {
  val c = createClient
  c.flushall

  "A hset" should "return correct result " in {
    c.hset("key0", "f0", "value0") shouldBe true
    c.hset("key0", "f0", "value1") shouldBe false
  }

  "A hget" should "return String result" in {
    c.hget[String]("key0", "f0") shouldEqual Some("value1")
    c.hget[String]("key0", "f1") shouldEqual None
  }

  "A hdel" should "return Boolean result" in {
    c.hdel("key0", "f0") shouldBe true
    c.hdel("key0", "f0") shouldBe false
  }

  "A hmset" should "return Boolean result" in {
    c.hmset("key1", ("f0", "Hello"), ("f1", "World")) shouldBe true
  }

  "A hmget" should "return String result" in {
    c.hmget[String]("key1", "f0", "f1") shouldEqual Map[String, String]("f0" -> "Hello", "f1" -> "World")
  }

  "A hincr" should "return Int result" in {
    c.hset("key1", "f2", 25)
    c.hincr("key1", "f2", 5) shouldEqual 30
    c.hincr("key1", "f2", -6) shouldEqual 24
    c.hincr("key1", "f2") shouldEqual 25
  }

  "A hexists" should "return Boolean result" in {
    c.hexists("key1", "f0") shouldBe true
    c.hexists("key1", "f4") shouldBe false
  }

  "A hlen" should "return Int result" in {
    c.hlen("key1") shouldEqual 3
  }

  "A keys" should "return Seq[String] result" in {
    c.hkeys("key1") shouldEqual Seq[String]("f0", "f1", "f2")
  }

  "A hvals" should "return Seq[String] result" in {
    c.hmset("key2", ("f0", 13), ("f1", 15))
    c.hvals[String]("key1") shouldEqual Seq[String]("Hello", "World", "25")
    c.hvals[Int]("key2") shouldEqual Seq[Int](13, 15)
  }

  "A hgetAll" should "return Map[String, T] result" in {
    c.hgetall[String]("key1") shouldEqual Map[String, String](("f0", "Hello"), ("f1", "World"), ("f2", "25"))
    c.hgetall[Int]("key2") shouldEqual Map[String, Int](("f0", 13), ("f1", 15))
  }

  "A hstrlen" should "return Int result" in {
    c.hstrlen("key1", "f0") shouldEqual 5
    c.hstrlen("key2", "f0") shouldEqual 2
  }

  "A hsetnx" should "return Boolean result" in {
    c.hset("key3", "f0", "Hello") shouldBe true
    c.hsetnx("key3", "f0", "Hello Vesteros") shouldBe false
    c.hsetnx("key3", "f3", "field3") shouldBe true
    c.hget[String]("key3", "f3") shouldEqual Some("field3")
  }

  "A hincrbyfloat" should "return Double result" in {
    c.hincrbyfloat[Double]("key1", "f2", 25.0) shouldEqual 50.0
    c.hincrbyfloat[Double]("key1", "f2", -24.0) shouldEqual 26.0
    c.hincrbyfloat[Double]("key1", "f2") shouldEqual 27.0
  }
}
