package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 22.07.16.
  */
class StringCommandsSpec extends FlatSpec with Matchers with TestClient {

  "An append method" should "return string length" in {
    client.append("key-append", "Hello") shouldEqual 5
    client.append("key-append", " World") shouldEqual 11
  }

  "A set method" should "return Boolean result" in {
    client.set("key-set", 3) shouldBe true
    client.set("key-set1", "Hello") shouldBe true
    client.set("key-set", 14) shouldBe true
  }

  "A get method" should "return Option[T]" in {
    client.get[Int]("key-get1") shouldEqual None
    client.set("key-get1", "String Value") shouldBe true
    client.get[String]("key-get1") shouldEqual Some("String Value")
  }

  "A set(keys*) method" should "return Boolean result" in {
    client.set[String](("key-multiset", "Hello"), ("key-multiset1", "World")) shouldBe true
    client.get[String]("key-multiset") shouldEqual Some("Hello")
    client.set[Int](("key-multiset", 13), ("key-multiset1", 15)) shouldBe true
    client.get[Int]("key-multiset") shouldEqual Some(13)
  }

  "A setNx method" should "return Boolean result" in {
    client.setNx("key-setNx", "Str") shouldBe true
    client.setNx("key-setNx", "Str") shouldBe false
  }

  "A setNx(keys) method" should "return Boolean result" in {
    client.setNx[String](("key-setNx-keys", "Hello"), ("key-setNx-keys1", "World")) shouldBe true
    client.setNx[String](("key-setNx-keys", "Hello"), ("key-setNx-keys1", "String")) shouldBe false
  }

  "A setXx method" should "return Boolean result" in {
    client.set("key-setXx", 15) shouldBe true
    client.setXx("key-setXx", 15, 2) shouldBe true
    client.get[Int]("key-setXx") shouldEqual Some(15)
    client.ttl("key-setXx") shouldEqual 2

    Thread.sleep(2000)

    client.ttl("key-setXx") shouldEqual -2
    client.get[Int]("key-setXx") shouldEqual None
  }

  "A get(keys*) method" should "return Seq(Option[T])" in {
    client.set[Int](("key-get-multi", 1), ("key-get-multi1", 2)) shouldBe true
    client.get[Int]("key-get-multi", "key-get-multi1") shouldEqual Seq(Some(1), Some(2))
  }

  "A mget method" should "return Map[String, T]" in {
    client.set[Int](("key-mget", 1), ("key-mget1", 2)) shouldBe true
    client.mget[Int]("key-mget", "key-mget1") shouldEqual Map[String, Int]("key-mget" -> 1, "key-mget1" -> 2)
    client.mget[Int]("key-mget", "key-mget1", "key-mget2") shouldEqual Map[String, Int]("key-mget" -> 1, "key-mget1" -> 2)
  }

  "A getrange method" should "return Option[T]" in {
    client.set("key-getrange", "This is test string") shouldBe true
    client.getrange[String]("key-getrange", 0, 4) shouldEqual Some("This ")
    client.getrange[String]("key-getrange", 0, -1) shouldEqual Some("This is test string")
    client.getrange[String]("key-getrange", -4, -1) shouldEqual Some("ring")
    client.getrange[String]("key-getrange", 5, 9) shouldEqual Some("is te")
  }

  "A getset method" should "return Option[T]" in {
    client.set("key-getset", "Hello") shouldBe true
    client.getset[String]("key-getset", "World") shouldEqual Some("Hello")
    client.get[String]("key-getset") shouldEqual Some("World")
  }

}
