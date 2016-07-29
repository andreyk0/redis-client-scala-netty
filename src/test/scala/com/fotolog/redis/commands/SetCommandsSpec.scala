package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 22.07.16.
  */
class SetCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A sadd method" should "correctly add a value to a set" in {
    client.sadd("key-sadd", 4) shouldEqual 1
    client.sadd("key-sadd", "Hello") shouldEqual 1
    client.sadd("key-sadd", 4) shouldEqual 0
  }

  "A smembers" should "correctly show set's member" in {
    client.sadd("key-smemmbers", "one")
    client.sadd("key-smemmbers", "two")
    client.smembers[String]("key-smemmbers") shouldEqual Set("one", "two")
  }

  "A srem method" should "correctly remove value from set" in {
    client.sadd("key-srem", "one")
    client.sadd("key-srem", "two")
    client.srem("key-srem", "one") shouldBe true
    client.srem("key-srem", "four") shouldBe false
  }

  "A spop method" should "randomly popup some value from set" in {
    client.sadd("key-spop", "one")
    client.sadd("key-spop", "two")
    client.sadd("key-spop", "three")

    val a = client.spop[String]("key-spop")
    a should contain oneOf("one", "two", "three")

    a match {
      case Some("one") => client.smembers[String]("key-spop") shouldEqual Set("two", "three")
      case Some("two") => client.smembers[String]("key-spop") shouldEqual Set("one", "three")
      case Some("three") => client.smembers[String]("key-spop") shouldEqual Set("one", "two")
      case _ => throw new MatchError("no matches in spop method test")
    }
  }

  "A smove method" should "move value from set to another set" in {
    client.sadd("key-smove", "str1")
    client.sadd("key-smove1","str2")
    client.smove[String]("key-smove", "key-smove1", "str1") shouldBe true
    client.smembers[String]("key-smove1") shouldEqual Set("str2", "str1")
    client.smembers[String]("key-smove") shouldEqual Set()
  }

  "A scard" should "get the number of element in set" in {
    client.sadd("key-scard", 4) shouldEqual 1
    client.sadd("key-scard", 8) shouldEqual 1
    client.sadd("key-scard", 1) shouldEqual 1
    client.scard("key-scard") shouldEqual 3
  }

  "A sismember" should " return if value is a member of the set" in {
    client.sadd("key-sismember", 4) shouldEqual 1
    client.sismember("key-sismember", 4) shouldBe true
  }

  "A sinter" should "return same element between sets" in {
    client.sadd("key-sinter", 4, 5, 6, 7) shouldEqual 4
    client.sadd("key-sinter1", 3, 2, 1, 4) shouldEqual 4
    client.sadd("key-sinter2", 4, 5, 8, 9) shouldEqual 4
    client.sinter[Int]("key-sinter", "key-sinter1") shouldEqual Set(4)
    client.sinter[Int]("key-sinter", "key-sinter2") shouldEqual Set(4, 5)
    client.sinter[Int]("key-sinter", "key-sinter1", "key-sinter2") shouldEqual Set(4)
  }

  "A sinterstore" should "return set that contain same element from another sets" in {
    client.sadd("key-sinterstore1", 3, 2, 1, 4) shouldEqual 4
    client.sadd("key-sinterstore2", 4, 2, 8, 9) shouldEqual 4
    client.sinterstore("key-sinterstore", "key-sinterstore1", "key-sinterstore2") shouldEqual 2
    client.smembers[Int]("key-sinterstore") shouldEqual Set(4, 2)
  }

  "A sunion" should "return resulting set from union of given sets" in {
    client.sadd("key-sunion", 4, 5, 6, 7) shouldEqual 4
    client.sadd("key-sunion1", 3, 2, 1, 4) shouldEqual 4
    client.sadd("key-sunion2", 4, 5, 8, 9) shouldEqual 4
    client.sunion[Int]("key-sunion", "key-sunion1", "key-sunion2") shouldEqual Set(1, 2, 3, 4, 5, 6, 7, 8, 9)
  }

  "A sunionstore" should "the number of elements from union of given sets" in {
    client.sadd("key-sunionstore1", 3, 2, 1, 4) shouldEqual 4
    client.sadd("key-sunionstore2", 4, 5, 8, 9) shouldEqual 4
    client.sunionstore[Int]("key-sunionstore", "key-sunionstore1", "key-sunionstore2") shouldEqual 7
    client.smembers[Int]("key-sunionstore") shouldEqual Set(1, 2, 3, 4, 5, 8, 9)
  }

  "A sdiff" should "return resulting set from difference of given sets" in {
    client.sadd("key-sdiff", 4, 5, 6, 7) shouldEqual 4
    client.sadd("key-sdiff1", 6, 7, 8, 9) shouldEqual 4
    client.sadd("key-sdiff2", 10, 5, 8, 9) shouldEqual 4
    client.sdiff[Int]("key-sdiff", "key-sdiff1", "key-sdiff2") shouldEqual Set(4)
  }

  "A sdiffstore" should "return difference on sets stored in destination set" in {
    client.sadd("key-sdiffstore1", 6, 7, 8, 9) shouldEqual 4
    client.sadd("key-sdiffstore2", 10, 5, 8, 9) shouldEqual 4
    client.sdiffstore[Int]("key-sdiffstore", "key-sdiffstore1", "key-sdiffstore2") shouldEqual 2
    client.smembers[Int]("key-sdiffstore") shouldEqual Set(6, 7)
  }

  "A srandmember" should "return random elem from given set" in {
    client.sadd("key-srandmember", 6, 7, 8, 9) shouldEqual 4
    client.srandmember[Int]("key-srandmember") should contain oneOf(6, 7, 8, 9)
  }
}
