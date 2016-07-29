package com.fotolog.redis.commands

import com.fotolog.redis._
import com.fotolog.redis.connections._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * http://redis.io/commands#string
 */
private[redis] trait StringCommands extends ClientCommands {
  import com.fotolog.redis.commands.ClientCommands._

  def appendAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Append(key, conv.write(value) )).map(integerResultAsInt)

  def append[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {appendAsync(key, value)(conv) }

  def decrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Decr(key, delta)).map(integerResultAsInt)
  def decr[T](key: String, delta: Int = 1): Int = await { decrAsync(key, delta) }

  def setAsync[T](key: String, value: T, expTime: Int = -1)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value), expTime)).map(okResultAsBoolean)

  def set[T](key: String, value: T, expTime: Int = -1)(implicit conv: BinaryConverter[T]): Boolean = await {
    setAsync(key, value, expTime)(conv)
  }

  def setNxAsync[T](key: String, value: T, expTime: Int = -1)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value), expTime, true)).map(okResultAsBoolean)

  def setNx[T](key: String, value: T, expTime: Int = -1)(implicit conv: BinaryConverter[T]): Boolean = await {
    setNxAsync(key, value, expTime)(conv)
  }

  def setXxAsync[T](key: String, value: T, expTime: Int = -1)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value), expTime, false, true)).map(okResultAsBoolean)

  def setXx[T](key: String, value: T, expTime: Int = -1)(implicit conv: BinaryConverter[T]): Boolean = await {
    setXxAsync(key, value, expTime)(conv)
  }

  def setNxAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetNx(kvs.map { kv => kv._1 -> conv.write(kv._2)})).map(integerResultAsBoolean)

  def setNx[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setNxAsync(kvs: _*)(conv) }

  def setAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(MSet(kvs.map { kv => kv._1 -> conv.write(kv._2)})).map(okResultAsBoolean)

  def set[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setAsync(kvs: _*)(conv) }

  def getAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Get(key)).map(bulkDataResultToOpt(conv))

  def getAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Seq[Option[T]]] = r.send(MGet(keys)).map {
    case BulkDataResult(data) => Seq(data.map(conv.read))
    case MultiBulkDataResult(results) => results.map { _.data.map(conv.read) }
    case x => throw new IllegalStateException("Invalid response got from server: " + x)
  }

  def get[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { getAsync(key)(conv) }
  def get[T](keys: String*)(implicit conv: BinaryConverter[T]): Seq[Option[T]] = await { getAsync(keys: _*)(conv) }

  def mgetAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] =
    r.send(MGet(keys)).map(multiBulkDataResultToMap(keys, conv))

  def mget[T](keys: String*)(implicit conv: BinaryConverter[T]): Map[String,T] = await { mgetAsync(keys: _*)(conv) }

  def getsetAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(GetSet(key, conv.write(value) )).map(bulkDataResultToOpt(conv))

  def getset[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Option[T] = await{ getsetAsync(key, value)(conv)}

  def incrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Incr(key, delta)).map(integerResultAsInt)
  def incr[T](key: String, delta: Int = 1): Int = await { incrAsync(key, delta) }

  def getrangeAsync[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Getrange(key, startOffset, endOffset)).map(bulkDataResultToOpt(conv))

  def getrange[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Option[T] =
    await { getrangeAsync(key, startOffset, endOffset)(conv) }
}
