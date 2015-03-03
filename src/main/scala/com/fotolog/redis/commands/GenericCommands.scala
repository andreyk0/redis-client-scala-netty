package com.fotolog.redis.commands

import com.fotolog.redis._
import com.fotolog.redis.connections._
import scala.collection.Set
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * http://redis.io/commands#generic
 * http://redis.io/commands#connection
 * http://redis.io/commands#transactions
 */
private[redis] trait GenericCommands extends ClientCommands {
  self: RedisClient =>

  import ClientCommands._

  def delAsync(key: String): Future[Int] = r.send(Del(key)).map(integerResultAsInt)

  def del(key: String): Int = await { delAsync(key) }

  def existsAsync(key: String): Future[Boolean] = r.send(Exists(key)).map(integerResultAsBoolean)
  def exists(key: String): Boolean = await(existsAsync(key))

  def expireAsync(key: String, seconds: Int): Future[Boolean] = r.send(Expire(key, seconds)).map(integerResultAsBoolean)
  def expire(key: String, seconds: Int): Boolean = await { expireAsync(key, seconds) }

  def keysAsync(pattern: String): Future[Set[String]] =
    r.send(Keys(pattern)).map(multiBulkDataResultToSet(BinaryConverter.StringConverter))

  def keys(pattern: String) = await(keysAsync(pattern))

  def keytypeAsync(key: String): Future[KeyType] = r.send(Type(key)).map {
    case SingleLineResult(s) => KeyType(s)
    case x => throw new IllegalStateException("Invalid response got from server: " + x)
  }

  def keytype(key: String): KeyType = await(keytypeAsync(key))

  def persistAsync(key: String): Future[Boolean] = r.send(Persist(key)).map(integerResultAsBoolean)
  def persist(key: String): Boolean = await { persistAsync(key) }

  def renameAsync(key: String, newKey: String, notExist: Boolean = true) =
    r.send(Rename(key, newKey, notExist)).map(integerResultAsBoolean)

  def rename(key: String, newKey: String, notExist: Boolean = true) = await {
    renameAsync(key, newKey, notExist)
  }


  def ttlAsync(key: String): Future[Int] = r.send(Ttl(key)).map(integerResultAsInt)
  def ttl(key: String): Int = await { ttlAsync(key) }

  def flushall = await { r.send(FlushAll()) }

  def ping(): Boolean = await {
    r.send(Ping()).map {
      case SingleLineResult("PONG") => true
      case _ => false
    }
  }

  def info: Map[String,String] = await {
    r.send(Info()).map {
      case BulkDataResult(Some(data)) =>
        val info = BinaryConverter.StringConverter.read(data)
        info.split("\r\n").map(_.split(":")).map(x => x(0) -> x(1)).toMap
      case x => throw new IllegalStateException("Invalid response got from server: " + x)
    }
  }


  def discardAsync() = r.send(Discard())
  def multiAsync() = r.send(Multi()).map(okResultAsBoolean)
  def execAsync() = r.send(Exec()).map(multiBulkDataResultToSet(BinaryConverter.StringConverter))
  def watchAsync(keys: String*) = r.send(Watch(keys:_*))
  def unwatchAsync() = r.send(Unwatch())

  def withTransaction(block: RedisClient => Unit) = {
    multiAsync()
    try {
      block(self)
      execAsync()
    } catch {
      case e: Exception =>
        discardAsync()
        throw e
    }
  }
}