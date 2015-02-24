package com.fotolog.redis

import com.fotolog.redis.commands._
import com.fotolog.redis.connections.{RedisConnection, InMemoryRedisConnection, Netty3RedisConnection}

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.{Future, Await}

object RedisClient {

  private final val DEFAULT_TIMEOUT = Duration(1, TimeUnit.MINUTES)

  def apply(host: String = "localhost", port: Int = 6379, timeout: Duration = DEFAULT_TIMEOUT) =
    if(host.startsWith("mem:")) {
      new RedisClient(new InMemoryRedisConnection(host.substring("mem:".length)), timeout)
    } else {
      new RedisClient(new Netty3RedisConnection(host, port), timeout)
    }

}

class RedisClient(val r: RedisConnection, val timeout: Duration) extends GenericCommands with StringCommands
                                             with HashCommands with ListCommands
                                             with SetCommands with ScriptingCommands {

  def isConnected: Boolean = r.isOpen
  def shutdown() { r.shutdown() }
  def await[T](f: Future[T]) = Await.result[T](f, timeout)
}
