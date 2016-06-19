package com.fotolog.redis

import java.util.concurrent.TimeUnit

import com.fotolog.redis
import com.fotolog.redis.commands._
import com.fotolog.redis.connections.{InMemoryRedisConnection, Netty3RedisConnection, RedisConnection}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object RedisClient {

  private final val DEFAULT_TIMEOUT = Duration(1, TimeUnit.MINUTES)

  implicit class RichFutureWrapper[T](val f: Future[T]) extends AnyVal {
    def get() = Await.result[T](f, DEFAULT_TIMEOUT)
  }

  @throws(classOf[AuthenticationException])
  def apply(host: String = "localhost",
            port: Int = 6379,
            password: Option[String] = None,
            timeout: Duration = DEFAULT_TIMEOUT) = {
    val client = if (host.startsWith("mem:")) {
      new RedisClient(new InMemoryRedisConnection(host.substring("mem:".length)), timeout)
    } else {
      // in case host specified as "host:port" then override port
      val (aHost, aPort) = if(host.contains(":")) {
        val Array(h, p) = host.split(":")
        (h, p.toInt)
      } else {
        (host, port)
      }

      new RedisClient(new Netty3RedisConnection(aHost, aPort), timeout)
    }

    for (pass <- password) {
      if (!client.auth(pass)) {
        throw new AuthenticationException("Authentication failed")
      }
    }

    client
  }

}

class RedisClient(val r: RedisConnection, val timeout: Duration) extends GenericCommands with StringCommands
                                             with HashCommands with ListCommands
                                             with SetCommands with ScriptingCommands with PubSubCommands
                                             with HyperLogLogCommands {

  def isConnected: Boolean = r.isOpen
  def shutdown() { r.shutdown() }
  def await[T](f: Future[T]) = Await.result[T](f, timeout)
}
