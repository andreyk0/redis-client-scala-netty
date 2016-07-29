package com.fotolog.redis.commands

import com.fotolog.redis._
import com.fotolog.redis.connections._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * http://redis.io/commands#hash
 */
private[redis] trait HashCommands extends ClientCommands {
  import com.fotolog.redis.commands.ClientCommands._

  def hsetAsync[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hset(key, field, conv.write(value))).map(integerResultAsBoolean)

  def hset[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { hsetAsync(key, field, value)(conv) }

  def hgetAsync[T](key: String, field: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Hget(key, field)).map(bulkDataResultToOpt(conv))

  def hget[T](key: String, field: String)(implicit conv: BinaryConverter[T]): Option[T] = await { hgetAsync(key, field)(conv) }

  def hmgetAsync[T](key: String, fields: String*)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] =
    r.send(Hmget(key, fields)).map(multiBulkDataResultToMap(fields, conv))

  def hmget[T](key: String, fields: String*)(implicit conv: BinaryConverter[T]): Map[String,T] =
    await { hmgetAsync(key, fields: _*)(conv) }

  def hmsetAsync[T](key: String, kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hmset(key, kvs.map{kv => kv._1 -> conv.write(kv._2)})).map(okResultAsBoolean)

  def hmset[T](key: String, kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { hmsetAsync(key, kvs: _*)(conv) }

  def hincrAsync(key: String, field: String, delta: Int = 1): Future[Int] =
    r.send(Hincrby(key, field, delta)).map(integerResultAsInt)

  def hincr(key: String, field: String, delta: Int = 1): Int = await { hincrAsync(key, field, delta) }

  def hexistsAsync(key: String, field: String): Future[Boolean] = r.send(Hexists(key, field)).map(integerResultAsBoolean)
  def hexists(key: String, field: String): Boolean = await { hexistsAsync(key, field) }

  def hdelAsync(key: String, field: String): Future[Boolean] = r.send(Hdel(key, field)).map(integerResultAsBoolean)
  def hdel(key: String, field: String): Boolean = await { hdelAsync(key, field) }

  def hlenAsync(key: String): Future[Int] = r.send(Hlen(key)).map(integerResultAsInt)
  def hlen(key: String): Int = await { hlenAsync(key) }

  def hkeysAsync(key: String): Future[Seq[String]] =
    r.send(Hkeys(key)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.StringConverter))

  def hkeys(key: String): Seq[String] = await { hkeysAsync(key) }

  def hvalsAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Seq[T]] =
    r.send(Hvals(key)).map(multiBulkDataResultToFilteredSeq(conv))

  def hvals[T](key: String)(implicit conv: BinaryConverter[T]): Seq[T] = await { hvalsAsync(key)(conv) }

  def hgetallAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] = r.send(Hgetall(key)).map {
    case MultiBulkDataResult(List()) =>
      Map()
    case MultiBulkDataResult(results) =>
      var take = false
      results.zip(results.tail).filter { (_) => take = !take; take }.filter {
        case (k, BulkDataResult(Some(_))) => true
        case (k, BulkDataResult(None)) => false
      }.map { kv => BinaryConverter.StringConverter.read(kv._1.data.get) -> conv.read(kv._2.data.get)}.toMap
    case unknown =>
      throw UnsupportedResponseException("Unsupported response type: " + unknown)
  }

  def hgetall[T](key: String)(implicit conv: BinaryConverter[T]): Map[String,T] = await { hgetallAsync(key)(conv) }

  def hstrlen[T](key: String, field: String): Int = await { hstrlenAsync(key, field)}

  def hstrlenAsync[T](key: String, field: String): Future[Int] = r.send(Hstrlen(key, field)).map(integerResultAsInt)

  def hsetnxAsync[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hsetnx(key, field, conv.write(value))).map(integerResultAsBoolean)

  def hsetnx[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { hsetnxAsync(key, field, value) }

  def hincrbyfloatAsync[T](key: String, field: String, delta: Double)(implicit conv: BinaryConverter[T]): Future[Double] =
    r.send(Hincrbyfloat(key, field, delta)).map(doubleResultAsDouble)

  def hincrbyfloat[T](key: String, field: String, delta: Double = 1.0)(implicit conv: BinaryConverter[T]): Double = await { hincrbyfloatAsync(key, field, delta)}

}
