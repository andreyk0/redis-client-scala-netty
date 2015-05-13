package com.fotolog.redis.commands

import com.fotolog.redis.BinaryConverter
import com.fotolog.redis.connections.{PfMerge, PfCount, PfAdd}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * http://redis.io/commands#hyperloglog
 */
private[redis] trait HyperLogLogCommands extends ClientCommands {
  import com.fotolog.redis.commands.ClientCommands._

  def pfaddAsync[T](key: String, fields: T*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(PfAdd(key, fields.map(conv.write))).map(integerResultAsBoolean)

  def pfadd[T](key: String, fields: T*)(implicit conv: BinaryConverter[T]): Boolean = await { pfaddAsync(key, fields:_*)(conv) }

  def pfcountAsync(key: String): Future[Int] = r.send(PfCount(key)).map(integerResultAsInt)

  def pfcount(key: String): Int = await { pfcountAsync(key) }

  def pfmergeAsync(dst: String, keys: String*): Future[Boolean] = r.send(PfMerge(dst, keys)).map(okResultAsBoolean)

  def pfmerge(dst: String, keys: String*): Boolean = await { pfmergeAsync(dst, keys:_*) }

}
