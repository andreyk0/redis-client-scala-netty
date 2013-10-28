package com.fotolog.redis

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.{Future, Await}

trait StringConversions {
    import RedisClientTypes._
    implicit def convertStringToByteArray(s: String): BinVal = s.getBytes
    implicit def convertByteArrayToString(b: BinVal): String = new String(b)
}
trait FixedIntConversions {
    implicit def convertIntToByteArray(i: Int): Array[Byte] = {
        Array[Byte](
            ((i     )&0xFF).asInstanceOf[Byte],
            ((i>>  8)&0xFF).asInstanceOf[Byte],
            ((i>> 16)&0xFF).asInstanceOf[Byte],
            ((i>> 24)&0xFF).asInstanceOf[Byte]
        )
    }
    implicit def convertByteArrayToInt(b: Array[Byte]): Int = {
        (b(0).asInstanceOf[Int] & 0xFF)       |
        (b(1).asInstanceOf[Int] & 0xFF) << 8  |
        (b(2).asInstanceOf[Int] & 0xFF) << 16 |
        (b(3).asInstanceOf[Int] & 0xFF) << 24
    }

    implicit def convertLongToByteArray(i: Long): Array[Byte] = {
        Array[Byte](
            ((i     )&0xFF).asInstanceOf[Byte],
            ((i>>  8)&0xFF).asInstanceOf[Byte],
            ((i>> 16)&0xFF).asInstanceOf[Byte],
            ((i>> 24)&0xFF).asInstanceOf[Byte],
            ((i>> 32)&0xFF).asInstanceOf[Byte],
            ((i>> 40)&0xFF).asInstanceOf[Byte],
            ((i>> 48)&0xFF).asInstanceOf[Byte],
            ((i>> 56)&0xFF).asInstanceOf[Byte]
        )
    }
    implicit def convertByteArrayToLong(b: Array[Byte]): Long = {
        (b(0).asInstanceOf[Long] & 0xFF)       |
        (b(1).asInstanceOf[Long] & 0xFF) <<  8 |
        (b(2).asInstanceOf[Long] & 0xFF) << 16 |
        (b(3).asInstanceOf[Long] & 0xFF) << 24 |
        (b(4).asInstanceOf[Long] & 0xFF) << 32 |
        (b(5).asInstanceOf[Long] & 0xFF) << 40 |
        (b(6).asInstanceOf[Long] & 0xFF) << 48 |
        (b(7).asInstanceOf[Long] & 0xFF) << 56
    }
}

class Conversions
object Conversions extends Conversions with StringConversions with FixedIntConversions


object RedisClient {
    import RedisClientTypes._
    import scala.collection.{ Set => SCSet }

    def apply(): RedisClient = new RedisClient(new RedisConnection)
    def apply(host: String, port: Int) = new RedisClient(new RedisConnection(host, port))

    private[redis] val integerResultAsBoolean: PartialFunction[Result, Boolean] = {
        case IntegerResult(1) => true
        case IntegerResult(0) => false
    }

    private[redis] val okResultAsBoolean: PartialFunction[Result, Boolean] = {
        case SingleLineResult("OK") => true
        // for cases where any other val should produce an error
    }

    private[redis] val integerResultAsInt: PartialFunction[Result, Int] = {
        case IntegerResult(x) => x
    }
    
    private[redis] def bulkDataResultToOpt[T](convert: (BinVal)=>T): PartialFunction[Result, Option[T]] = {
        case BulkDataResult(data) => data match {
            case None => None
            case Some(d) => Some(convert(d))
        }
    }
    
    private[redis] def multiBulkDataResultToFilteredSeq[T](convert: (BinVal)=>T): PartialFunction[Result, Seq[T]] = {
        case MultiBulkDataResult(results) => results.filter{ r => r match {
                case BulkDataResult(Some(_)) => true
                case BulkDataResult(None) => false
        }}.map{ r => convert(r.data.get)}
    }

    private[redis] def multiBulkDataResultToSet[T](convert: (BinVal)=>T): PartialFunction[Result, SCSet[T]] = {
        case MultiBulkDataResult(results) => SCSet.empty[T] ++ results.filter{ r => r match {
                case BulkDataResult(Some(_)) => true
                case BulkDataResult(None) => false
        }}.map{ r => convert(r.data.get)}
    }

    private[redis] def multiBulkDataResultToMap[T](keys: Seq[String], convert: (BinVal)=>T): PartialFunction[Result, Map[String,T]] = {
        case BulkDataResult(data) => data match {
            case None => Map()
            case Some(d) => Map(keys.head -> convert(d))
        }
        case MultiBulkDataResult(results) => {
            Map.empty[String, T] ++
            keys.zip(results).filter{ kv => kv match {
                case (k, BulkDataResult(Some(_))) => true
                case (k, BulkDataResult(None)) => false
            }}.map{ kv => kv._1 -> convert(kv._2.data.get)}
        }
    }
}

class RedisClient(val r: RedisConnection) {

    import scala.concurrent.ExecutionContext.Implicits.global

    import RedisClientTypes._
    import RedisClient.integerResultAsBoolean
    import RedisClient.integerResultAsInt
    import RedisClient.okResultAsBoolean
    import RedisClient.bulkDataResultToOpt
    import RedisClient.multiBulkDataResultToFilteredSeq
    import RedisClient.multiBulkDataResultToSet
    import RedisClient.multiBulkDataResultToMap
    import Conversions._
    import scala.collection.{ Set => SCSet }

    val timeout = Duration(1, TimeUnit.MINUTES)
    def isConnected(): Boolean = r.isOpen
    def shutdown() { r.shutdown }
    
    def flushall = await{ r.send(FlushAll()) }

    def ping(): Boolean = await {
      r.send(Ping()).map {
        case SingleLineResult("PONG") => true
      }
    }
    
    def info: Map[String,String] = await {
      r.send(Info()).map {
        case BulkDataResult(Some(data)) =>
            val info = convertByteArrayToString(data)
            Map.empty[String,String] ++ info.split("\r\n").map{_.split(":")}.map{x=>x(0)->x(1)}
      }
    }
    
    def keytypeAsync(key: String): Future[KeyType] = r.send(Type(key)).map { case SingleLineResult(s) => KeyType(s) }
    def keytype(key: String): KeyType = await(keytypeAsync(key))

    def existsAsync(key: String): Future[Boolean] = r.send(Exists(key)).map(integerResultAsBoolean)
    def exists(key: String): Boolean = await(existsAsync(key))
    def ? (key: String): Boolean = exists(key)
    
    def setAsync[T](key: String, value: T)(implicit convert:(T)=>BinVal): Future[Boolean] =
      r.send(Set(key -> convert(value))).map(okResultAsBoolean)

    def set[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = await { setAsync(key, value)(convert) }

    def setAsync[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Future[Boolean] =
      r.send(Set(kvs.map{kv => kv._1 -> convert(kv._2)} : _*)).map(okResultAsBoolean)

    def set[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = await { setAsync(kvs: _*)(convert) }

    def setAsync[T](key: String, expiration: Int, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
      r.send(SetEx(key, expiration, convert(value))).map(okResultAsBoolean)

    def set[T](key: String, expiration: Int, value: T)(implicit convert: (T)=>BinVal): Boolean =
      await{ setAsync(key, expiration, value)(convert) }

    def delAsync(key: String): Future[Boolean] = r.send(Del(key)).map(integerResultAsBoolean)
    def del(key: String): Boolean = await { delAsync(key) }

    def getAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] =
      r.send(Get(key)).map(bulkDataResultToOpt(convert))

    def getAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[Seq[Option[T]]] = r.send(MGet(keys: _*)).map {
        case BulkDataResult(data) => data match {
            case None => Seq(None)
            case Some(data) => Seq(Some(convert(data)))
        }
        case MultiBulkDataResult(results) => results.map { _.data match {
                case None => None
                case Some(bytes) => Some(convert(bytes))
            }
        }
    }

    def get[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = await { getAsync(key)(convert) }
    def get[T](keys: String*)(implicit convert: (BinVal)=>T): Seq[Option[T]] = await { getAsync(keys: _*)(convert) }
    def apply[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = get(key)(convert)

    def mgetAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[Map[String,T]] =
      r.send( MGet(keys: _*)).map(multiBulkDataResultToMap(keys,convert))

    def mget[T](keys: String*)(implicit convert: (BinVal)=>T): Map[String,T] = await { mgetAsync(keys: _*)(convert) }

    def setnxAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
      r.send(SetNx(key -> convert(value))).map(integerResultAsBoolean)

    def setnxAsync[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Future[Boolean] =
      r.send(SetNx(kvs.map{kv => kv._1 -> convert(kv._2)} : _*)).map(integerResultAsBoolean)

    def setnx[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean =
      await { setnxAsync(key, value)(convert) }

    def setnx[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = await { setnxAsync(kvs: _*)(convert) }

    def getsetAsync[T](key: String, value: T)(implicit convertTo: (T)=>BinVal, convertFrom: (BinVal)=>T): Future[Option[T]] =
      r.send(GetSet(key -> convertTo(value))).map(bulkDataResultToOpt(convertFrom))

    def getset[T](key: String, value: T)(implicit convertTo: (T)=>BinVal, convertFrom: (BinVal)=>T): Option[T] =
      await { getsetAsync(key, value)(convertTo, convertFrom) }

    def incrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Incr(key, delta)).map(integerResultAsInt)
    def incr[T](key: String, delta: Int = 1): Int = await { incrAsync(key, delta) }

    def decrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Decr(key, delta)).map(integerResultAsInt)
    def decr[T](key: String, delta: Int = 1): Int = await { decrAsync(key, delta) }
    
    def appendAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Int] =
      r.send(Append(key -> convert(value))).map(integerResultAsInt)

    def append[T](key: String, value: T)(implicit convert: (T)=>BinVal): Int = await {appendAsync(key, value)(convert) }

    def substrAsync[T](key: String, startOffset: Int, endOffset: Int)(implicit convert: (BinVal)=>T): Future[Option[T]] =
      r.send(Substr(key, startOffset, endOffset)).map(bulkDataResultToOpt(convert))

    def substr[T](key: String, startOffset: Int, endOffset: Int)(implicit convert: (BinVal)=>T): Option[T] =
      await { substrAsync(key, startOffset, endOffset)(convert) }
    
    def expireAsync(key: String, seconds: Int): Future[Boolean] = r.send(Expire(key, seconds)).map(integerResultAsBoolean)
    def expire(key: String, seconds: Int): Boolean = await { expireAsync(key, seconds) }

    def persistAsync(key: String): Future[Boolean] = r.send(Persist(key)).map(integerResultAsBoolean)
    def persist(key: String): Boolean = await { persistAsync(key) }

    def rpushAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Int] =
      r.send(Rpush(key, convert(value))).map(integerResultAsInt)

    def rpush[T](key: String, value: T)(implicit convert: (T)=>BinVal): Int = await { rpushAsync(key, value) }

    def lpushAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Int] =
      r.send(Lpush(key, convert(value))).map(integerResultAsInt)

    def lpush[T](key: String, value: T)(implicit convert: (T)=>BinVal): Int = await {  lpushAsync(key, value) }

    def llenAsync[T](key: String): Future[Int] = r.send(Llen(key)).map(integerResultAsInt)
    def llen[T](key: String): Int = await {  llenAsync(key) }

    def lrangeAsync[T](key: String, start: Int, end: Int)(implicit convert: (BinVal)=>T): Future[Seq[T]] =
      r.send(Lrange(key, start, end)).map(multiBulkDataResultToFilteredSeq(convert))

    def lrange[T](key: String, start: Int, end: Int)(implicit convert: (BinVal)=>T): Seq[T] = await {  lrangeAsync(key, start, end)(convert) }

    def ltrimAsync(key: String, start: Int, end: Int): Future[Boolean] =
      r.send(Ltrim(key, start, end)).map(okResultAsBoolean)

    def ltrim(key: String, start: Int, end: Int): Boolean = await { ltrimAsync(key, start, end) }

    def lindexAsync[T](key: String, idx: Int)(implicit convert: (BinVal)=>T): Future[Option[T]] =
      r.send(Lindex(key, idx)).map(bulkDataResultToOpt(convert))

    def lindex[T](key: String, idx: Int)(implicit convert: (BinVal)=>T): Option[T] = await { lindexAsync(key, idx)(convert) }

    def lsetAsync[T](key: String, idx: Int, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
      r.send(Lset(key, idx, convert(value))).map(okResultAsBoolean)

    def lset[T](key: String, idx: Int, value: T)(implicit convert: (T)=>BinVal): Boolean = await {  lsetAsync(key, idx, value)(convert) }

    def lremAsync[T](key: String, count: Int, value: T)(implicit convert: (T)=>BinVal): Future[Int] =
      r.send(Lrem(key, count, convert(value))).map(integerResultAsInt)

    def lrem[T](key: String, count: Int, value: T)(implicit convert: (T)=>BinVal): Int = await {  lremAsync(key, count, value)(convert) }

    def lpopAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = r.send(Lpop(key)).map(bulkDataResultToOpt(convert))
    def lpop[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = await {  lpopAsync(key)(convert) }

    def rpopAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = r.send(Rpop(key)).map(bulkDataResultToOpt(convert))
    def rpop[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = await { rpopAsync(key)(convert) }

    def rpoplpushAsync[T](srcKey: String, destKey: String)(implicit convert: (BinVal)=>T): Future[Option[T]] =
      r.send(RpopLpush(srcKey, destKey)).map(bulkDataResultToOpt(convert))

    def rpoplpush[T](srcKey: String, destKey: String)(implicit convert: (BinVal)=>T): Option[T] = await { rpoplpushAsync(srcKey, destKey)(convert) }

    def hsetAsync[T](key: String, field: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
      r.send(Hset(key, field, convert(value))).map(integerResultAsBoolean)

    def hset[T](key: String, field: String, value: T)(implicit convert: (T)=>BinVal): Boolean = await { hsetAsync(key, field, value)(convert) }

    def hgetAsync[T](key: String, field: String)(implicit convert: (BinVal)=>T): Future[Option[T]] =
      r.send(Hget(key, field)).map(bulkDataResultToOpt(convert))

  def hget[T](key: String, field: String)(implicit convert: (BinVal)=>T): Option[T] = await { hgetAsync(key, field)(convert) }

  def hmgetAsync[T](key: String, fields: String*)(implicit convert: (BinVal)=>T): Future[Map[String,T]] =
    r.send(Hmget(key, fields: _*)).map(multiBulkDataResultToMap(fields, convert))

  def hmget[T](key: String, fields: String*)(implicit convert: (BinVal)=>T): Map[String,T] =
    await { hmgetAsync(key, fields: _*)(convert) }

  def hmsetAsync[T](key: String, kvs: (String,T)*)(implicit convert: (T)=>BinVal): Future[Boolean] =
    r.send(Hmset(key, kvs.map{kv => kv._1 -> convert(kv._2)} : _*)).map(okResultAsBoolean)

  def hmset[T](key: String, kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = await { hmsetAsync(key, kvs: _*)(convert) }

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
    r.send(Hkeys(key)).map(multiBulkDataResultToFilteredSeq(convertByteArrayToString))

  def hkeys(key: String): Seq[String] = await { hkeysAsync(key) }

  def hvalsAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Seq[T]] =
    r.send(Hvals(key)).map(multiBulkDataResultToFilteredSeq(convert))

  def hvals[T](key: String)(implicit convert: (BinVal)=>T): Seq[T] = await { hvalsAsync(key)(convert) }
    
  def hgetallAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Map[String,T]] = r.send(Hgetall(key)).map {
      case MultiBulkDataResult(List()) => Map.empty[String,T]
      case MultiBulkDataResult(results) => Map.empty[String, T] ++ {
          var take = false
          results.zip(results.tail).filter{ (_) => take = !take; take }.filter{ kv => kv match {
              case (k, BulkDataResult(Some(_))) => true
              case (k, BulkDataResult(None)) => false
          }}.map{ kv => convertByteArrayToString(kv._1.data.get) -> convert(kv._2.data.get)}
      }
  }

  def hgetall[T](key: String)(implicit convert: (BinVal)=>T): Map[String,T] = await { hgetallAsync(key)(convert) }

  def saddAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
    r.send(Sadd(key->convert(value))).map(integerResultAsBoolean)

  def sadd[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = await { saddAsync(key,value)(convert) }

  def sremAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
    r.send(Srem(key->convert(value))).map(integerResultAsBoolean)

  def srem[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = await { sremAsync(key,value)(convert) }

  def spopAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] =
    r.send(Spop(key)).map(bulkDataResultToOpt(convert))

  def spop[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = await { spopAsync(key)(convert) }

  def smoveAsync[T](srcKey: String, destKey: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
    r.send(Smove(srcKey, destKey, convert(value))).map(integerResultAsBoolean)

  def smove[T](srcKey: String, destKey: String, value: T)(implicit convert: (T)=>BinVal): Boolean =
    await { smoveAsync(srcKey, destKey, value)(convert) }

  def scardAsync(key: String): Future[Int] = r.send(Scard(key)).map(integerResultAsInt)
  def scard(key: String): Int = await { scardAsync(key) }

  def sismemberAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] =
    r.send(Sismember(key->convert(value))).map(integerResultAsBoolean)

  def sismember[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean =
    await { sismemberAsync(key, value)(convert) }

  def sinterAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[SCSet[T]] =
    r.send(Sinter(keys: _*)).map(multiBulkDataResultToSet(convert))

  def sinter[T](keys: String*)(implicit convert: (BinVal)=>T): SCSet[T] = await { sinterAsync(keys: _*)(convert) }

  def sinterstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sinterstore(destKey, keys: _*)).map(integerResultAsInt)

  def sinterstore[T](destKey: String, keys: String*): Int = await { sinterstoreAsync(destKey, keys: _*) }

  def sunionAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[SCSet[T]] =
    r.send(Sunion(keys: _*)).map(multiBulkDataResultToSet(convert))

  def sunion[T](keys: String*)(implicit convert: (BinVal)=>T): SCSet[T] = await { sunionAsync(keys: _*)(convert) }

  def sunionstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sunionstore(destKey, keys: _*)).map(integerResultAsInt)

  def sunionstore[T](destKey: String, keys: String*): Int = await { sunionstoreAsync(destKey, keys: _*) }

  def sdiffAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[SCSet[T]] =
    r.send(Sdiff(keys: _*)).map(multiBulkDataResultToSet(convert))

  def sdiff[T](keys: String*)(implicit convert: (BinVal)=>T): SCSet[T] = await { sdiffAsync(keys: _*)(convert) }

  def sdiffstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sdiffstore(destKey, keys: _*)).map(integerResultAsInt)

  def sdiffstore[T](destKey: String, keys: String*): Int = await { sdiffstoreAsync(destKey, keys: _*) }

  def smembersAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[SCSet[T]] =
    r.send(Smembers(key)).map(multiBulkDataResultToSet(convert))

  def smembers[T](key: String)(implicit convert: (BinVal)=>T): SCSet[T] = await { smembersAsync(key)(convert) }

  def srandmemberAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] =
    r.send(Srandmember(key)).map(bulkDataResultToOpt(convert))

  def srandmember[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = await { srandmemberAsync(key)(convert) }

  def await[T](f: Future[T]) = Await.result[T](f, timeout)
}
