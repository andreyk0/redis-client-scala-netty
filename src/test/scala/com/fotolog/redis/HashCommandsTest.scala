package com.fotolog.redis

/**
  * Created by faiaz on 12.07.16.
  */
import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.fotolog.redis.commands.HashCommands


@RunWith(classOf[JUnitRunner])
class HashCommandsTest extends FunSuite with TestClient {
  trait HashCommTest{
    val c = createClient
    c.flushall
    c.hset("hstrlen", "f0", "HelloTest")
    c.hset("hstrlen", "f1", "-256")
    c.hset("hstrlen", "f2", "12#$%^")
    c.hset("hincrbyfloat", "f0", 25.0)
    c.hset("hincrbyfloat", "f1", 2.0)

  }


  test("hstrlen test") {
    new HashCommTest {
      assert(c.hstrlen("hstrlen", "f0") === 9)
      assert(c.hstrlen("hstrlen", "f1") === 4)
      assert(c.hstrlen("hstrlen", "f2") === 6)
    }
  }

  test("hincrbyfloat test") {
    new HashCommTest {
      assert(c.hincrbyfloat[Double]("hincrbyfloat", "f0", 5.0) === 30.0)
      assert(c.hincrbyfloat[Double]("hincrbyfloat", "f1", -4.0) === -2.0)
    }
  }

  test("hsetnx test") {
    new HashCommTest {
      assert(c.hsetnx("hstrlen", "f0", "gagag") === 0)
      assert(c.hsetnx("hstrlen", "f3", "gagag") === 1)
    }
  }
}
