package com.fotolog.redis.commands

import com.fotolog.redis._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * http://redis.io/commands#string
 */
private[redis] trait StringCommands extends ClientCommands {
  import ClientCommands._

  def appendAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Append(key, conv.write(value) )).map(integerResultAsInt)

  def append[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {appendAsync(key, value)(conv) }

  def decrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Decr(key, delta)).map(integerResultAsInt)
  def decr[T](key: String, delta: Int = 1): Int = await { decrAsync(key, delta) }

  def setAsync[T](key: String, value: T)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value))).map(okResultAsBoolean)

  def set[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { setAsync(key, value)(conv) }

  def setAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(MSet(kvs.map { kv => kv._1 -> conv.write(kv._2)} : _*)).map(okResultAsBoolean)

  def set[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setAsync(kvs: _*)(conv) }

  def setAsync[T](key: String, expiration: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetEx(key, expiration, conv.write(value))).map(okResultAsBoolean)

  def set[T](key: String, expiration: Int, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await{ setAsync(key, expiration, value)(conv) }


  def getAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Get(key)).map(bulkDataResultToOpt(conv))

  def getAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Seq[Option[T]]] = r.send(MGet(keys: _*)).map {
    case BulkDataResult(data) => Seq(data.map(conv.read))
    case MultiBulkDataResult(results) => results.map { _.data.map(conv.read) }
    case x => throw new IllegalStateException("Invalid response got from server: " + x)
  }

  def get[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { getAsync(key)(conv) }
  def get[T](keys: String*)(implicit conv: BinaryConverter[T]): Seq[Option[T]] = await { getAsync(keys: _*)(conv) }

  def mgetAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] =
    r.send( MGet(keys: _*)).map(multiBulkDataResultToMap(keys, conv))

  def mget[T](keys: String*)(implicit conv: BinaryConverter[T]): Map[String,T] = await { mgetAsync(keys: _*)(conv) }

  def setnxAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetNx(key -> conv.write(value))).map(integerResultAsBoolean)

  def setnxAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetNx(kvs.map{kv => kv._1 -> conv.write(kv._2)} : _*)).map(integerResultAsBoolean)

  def setnx[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { setnxAsync(key, value)(conv) }

  def setnxAsync[T](key: String, expiration: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetNx(key -> conv.write(value))).map(integerResultAsBoolean)

  def setnx[T](key: String, expiration: Int, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { setnxAsync(key, expiration, value)(conv) }

  def setnx[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setnxAsync(kvs: _*)(conv) }

  def getsetAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(GetSet(key, conv.write(value) )).map(bulkDataResultToOpt(conv))

  def getset[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Option[T] = await{ getsetAsync(key, value)(conv)}

  def incrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Incr(key, delta)).map(integerResultAsInt)
  def incr[T](key: String, delta: Int = 1): Int = await { incrAsync(key, delta) }

  def substrAsync[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Substr(key, startOffset, endOffset)).map(bulkDataResultToOpt(conv))

  def substr[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Option[T] =
    await { substrAsync(key, startOffset, endOffset)(conv) }
}
