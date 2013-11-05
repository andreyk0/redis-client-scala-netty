package com.fotolog.redis

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.{Future, Await}

object RedisClient {
    import scala.collection.{ Set => SCSet }

    def apply(): RedisClient = new RedisClient(new RedisConnection)
    def apply(host: String, port: Int = 6379) = new RedisClient(new RedisConnection(host, port))

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
    
    private[redis] def bulkDataResultToOpt[T](convert: BinaryConverter[T]): PartialFunction[Result, Option[T]] = {
        case BulkDataResult(data) => data.map(convert.read)
    }
    
    private[redis] def multiBulkDataResultToFilteredSeq[T](conv: BinaryConverter[T]): PartialFunction[Result, Seq[T]] = {
        case MultiBulkDataResult(results) => results.filter {
          case BulkDataResult(Some(_)) => true
          case BulkDataResult(None) => false
        }.map{ r => conv.read(r.data.get)}
    }

    private[redis] def multiBulkDataResultToSet[T](conv: BinaryConverter[T]): PartialFunction[Result, SCSet[T]] = {
        case MultiBulkDataResult(results) => results.filter {
          case BulkDataResult(Some(_)) => true
          case BulkDataResult(None) => false
        }.map{ r => conv.read(r.data.get)}.toSet
    }

    private[redis] def multiBulkDataResultToMap[T](keys: Seq[String], conv: BinaryConverter[T]): PartialFunction[Result, Map[String,T]] = {
        case BulkDataResult(data) => data match {
            case None => Map()
            case Some(d) => Map(keys.head -> conv.read(d))
        }
        case MultiBulkDataResult(results) => {
            keys.zip(results).filter {
              case (k, BulkDataResult(Some(_))) => true
              case (k, BulkDataResult(None)) => false
            }.map { kv => kv._1 -> conv.read(kv._2.data.get) }.toMap
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
  import scala.collection.{ Set => SCSet }

  private[this] final val timeout = Duration(1, TimeUnit.MINUTES)

  def isConnected: Boolean = r.isOpen
  def shutdown() { r.shutdown() }

  def flushall = await{ r.send(FlushAll()) }

  def ping(): Boolean = await {
    r.send(Ping()).map {
      case SingleLineResult("PONG") => true
    }
  }

  def info: Map[String,String] = await {
    r.send(Info()).map {
      case BulkDataResult(Some(data)) =>
          val info = BinaryConverter.StringConverter.read(data)
          info.split("\r\n").map(_.split(":")).map(x => x(0) -> x(1)).toMap
    }
  }

  def keytypeAsync(key: String): Future[KeyType] = r.send(Type(key)).map { case SingleLineResult(s) => KeyType(s) }
  def keytype(key: String): KeyType = await(keytypeAsync(key))

  def existsAsync(key: String): Future[Boolean] = r.send(Exists(key)).map(integerResultAsBoolean)
  def exists(key: String): Boolean = await(existsAsync(key))

  def setAsync[T](key: String, value: T)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(Set(key, conv.write(value))).map(okResultAsBoolean)

  def set[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { setAsync(key, value)(conv) }

  def setAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(MSet(kvs.map { kv => kv._1 -> conv.write(kv._2)} : _*)).map(okResultAsBoolean)

  def set[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setAsync(kvs: _*)(conv) }

  def setAsync[T](key: String, expiration: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetEx(key, expiration, conv.write(value))).map(okResultAsBoolean)

  def set[T](key: String, expiration: Int, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await{ setAsync(key, expiration, value)(conv) }

  def delAsync(key: String): Future[Boolean] = r.send(Del(key)).map(integerResultAsBoolean)
  def del(key: String): Boolean = await { delAsync(key) }

  def getAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Get(key)).map(bulkDataResultToOpt(conv))

  def getAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Seq[Option[T]]] = r.send(MGet(keys: _*)).map {
      case BulkDataResult(data) => Seq(data.map(conv.read))
      case MultiBulkDataResult(results) => results.map { _.data.map(conv.read) }
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

  def setnx[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setnxAsync(kvs: _*)(conv) }

  def getsetAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(GetSet(key -> conv.write(value))).map(bulkDataResultToOpt(conv))

  def getset[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Option[T] = await{ getsetAsync(key, value)(conv)}

  def incrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Incr(key, delta)).map(integerResultAsInt)
  def incr[T](key: String, delta: Int = 1): Int = await { incrAsync(key, delta) }

  def decrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Decr(key, delta)).map(integerResultAsInt)
  def decr[T](key: String, delta: Int = 1): Int = await { decrAsync(key, delta) }

  def appendAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Append(key -> conv.write(value))).map(integerResultAsInt)

  def append[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {appendAsync(key, value)(conv) }

  def substrAsync[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Substr(key, startOffset, endOffset)).map(bulkDataResultToOpt(conv))

  def substr[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Option[T] =
    await { substrAsync(key, startOffset, endOffset)(conv) }

  def expireAsync(key: String, seconds: Int): Future[Boolean] = r.send(Expire(key, seconds)).map(integerResultAsBoolean)
  def expire(key: String, seconds: Int): Boolean = await { expireAsync(key, seconds) }

  def persistAsync(key: String): Future[Boolean] = r.send(Persist(key)).map(integerResultAsBoolean)
  def persist(key: String): Boolean = await { persistAsync(key) }

  def rpushAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Rpush(key, conv.write(value))).map(integerResultAsInt)

  def rpush[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await { rpushAsync(key, value)(conv) }

  def lpushAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Lpush(key, conv.write(value))).map(integerResultAsInt)

  def lpush[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {  lpushAsync(key, value)(conv) }

  def llenAsync[T](key: String): Future[Int] = r.send(Llen(key)).map(integerResultAsInt)
  def llen[T](key: String): Int = await {  llenAsync(key) }

  def lrangeAsync[T](key: String, start: Int, end: Int)(implicit conv: BinaryConverter[T]): Future[Seq[T]] =
    r.send(Lrange(key, start, end)).map(multiBulkDataResultToFilteredSeq(conv))

  def lrange[T](key: String, start: Int, end: Int)(implicit conv: BinaryConverter[T]): Seq[T] =
    await { lrangeAsync(key, start, end)(conv) }

  def ltrimAsync(key: String, start: Int, end: Int): Future[Boolean] =
    r.send(Ltrim(key, start, end)).map(okResultAsBoolean)

  def ltrim(key: String, start: Int, end: Int): Boolean = await { ltrimAsync(key, start, end) }

  def lindexAsync[T](key: String, idx: Int)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Lindex(key, idx)).map(bulkDataResultToOpt(conv))

  def lindex[T](key: String, idx: Int)(implicit conv: BinaryConverter[T]): Option[T] = await { lindexAsync(key, idx)(conv) }

  def lsetAsync[T](key: String, idx: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Lset(key, idx, conv.write(value))).map(okResultAsBoolean)

  def lset[T](key: String, idx: Int, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { lsetAsync(key, idx, value)(conv) }

  def lremAsync[T](key: String, count: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Lrem(key, count, conv.write(value))).map(integerResultAsInt)

  def lrem[T](key: String, count: Int, value: T)(implicit conv: BinaryConverter[T]): Int =
    await { lremAsync(key, count, value)(conv) }

  def lpopAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Lpop(key)).map(bulkDataResultToOpt(conv))

  def lpop[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { lpopAsync(key)(conv) }

  def rpopAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Rpop(key)).map(bulkDataResultToOpt(conv))

  def rpop[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { rpopAsync(key)(conv) }

  def rpoplpushAsync[T](srcKey: String, destKey: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(RpopLpush(srcKey, destKey)).map(bulkDataResultToOpt(conv))

  def rpoplpush[T](srcKey: String, destKey: String)(implicit conv: BinaryConverter[T]): Option[T] =
    await { rpoplpushAsync(srcKey, destKey)(conv) }

  def hsetAsync[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hset(key, field, conv.write(value))).map(integerResultAsBoolean)

  def hset[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { hsetAsync(key, field, value)(conv) }

  def hgetAsync[T](key: String, field: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Hget(key, field)).map(bulkDataResultToOpt(conv))

  def hget[T](key: String, field: String)(implicit conv: BinaryConverter[T]): Option[T] = await { hgetAsync(key, field)(conv) }

  def hmgetAsync[T](key: String, fields: String*)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] =
    r.send(Hmget(key, fields: _*)).map(multiBulkDataResultToMap(fields, conv))

  def hmget[T](key: String, fields: String*)(implicit conv: BinaryConverter[T]): Map[String,T] =
    await { hmgetAsync(key, fields: _*)(conv) }

  def hmsetAsync[T](key: String, kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hmset(key, kvs.map{kv => kv._1 -> conv.write(kv._2)} : _*)).map(okResultAsBoolean)

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
      case MultiBulkDataResult(List()) => Map()
      case MultiBulkDataResult(results) => {
          var take = false
          results.zip(results.tail).filter { (_) => take = !take; take }.filter {
            case (k, BulkDataResult(Some(_))) => true
            case (k, BulkDataResult(None)) => false
          }.map { kv => BinaryConverter.StringConverter.read(kv._1.data.get) -> conv.read(kv._2.data.get)}.toMap
      }
  }

  def hgetall[T](key: String)(implicit conv: BinaryConverter[T]): Map[String,T] = await { hgetallAsync(key)(conv) }

  def saddAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Sadd(key -> conv.write(value))).map(integerResultAsBoolean)

  def sadd[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { saddAsync(key,value)(conv) }

  def sremAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Srem(key -> conv.write(value))).map(integerResultAsBoolean)

  def srem[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { sremAsync(key,value)(conv) }

  def spopAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Spop(key)).map(bulkDataResultToOpt(conv))

  def spop[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { spopAsync(key)(conv) }

  def smoveAsync[T](srcKey: String, destKey: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Smove(srcKey, destKey, conv.write(value))).map(integerResultAsBoolean)

  def smove[T](srcKey: String, destKey: String, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { smoveAsync(srcKey, destKey, value)(conv) }

  def scardAsync(key: String): Future[Int] = r.send(Scard(key)).map(integerResultAsInt)
  def scard(key: String): Int = await { scardAsync(key) }

  def sismemberAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Sismember(key -> conv.write(value))).map(integerResultAsBoolean)

  def sismember[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { sismemberAsync(key, value)(conv) }

  def sinterAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[SCSet[T]] =
    r.send(Sinter(keys: _*)).map(multiBulkDataResultToSet(conv))

  def sinter[T](keys: String*)(implicit conv: BinaryConverter[T]): SCSet[T] = await { sinterAsync(keys: _*)(conv) }

  def sinterstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sinterstore(destKey, keys: _*)).map(integerResultAsInt)

  def sinterstore[T](destKey: String, keys: String*): Int = await { sinterstoreAsync(destKey, keys: _*) }

  def sunionAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[SCSet[T]] =
    r.send(Sunion(keys: _*)).map(multiBulkDataResultToSet(conv))

  def sunion[T](keys: String*)(implicit conv: BinaryConverter[T]): SCSet[T] = await { sunionAsync(keys: _*)(conv) }

  def sunionstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sunionstore(destKey, keys: _*)).map(integerResultAsInt)

  def sunionstore[T](destKey: String, keys: String*): Int = await { sunionstoreAsync(destKey, keys: _*) }

  def sdiffAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[SCSet[T]] =
    r.send(Sdiff(keys: _*)).map(multiBulkDataResultToSet(conv))

  def sdiff[T](keys: String*)(implicit conv: BinaryConverter[T]): SCSet[T] = await { sdiffAsync(keys: _*)(conv) }

  def sdiffstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sdiffstore(destKey, keys: _*)).map(integerResultAsInt)

  def sdiffstore[T](destKey: String, keys: String*): Int = await { sdiffstoreAsync(destKey, keys: _*) }

  def smembersAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[SCSet[T]] =
    r.send(Smembers(key)).map(multiBulkDataResultToSet(conv))

  def smembers[T](key: String)(implicit conv: BinaryConverter[T]): SCSet[T] = await { smembersAsync(key)(conv) }

  def srandmemberAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Srandmember(key)).map(bulkDataResultToOpt(conv))

  def srandmember[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { srandmemberAsync(key)(conv) }

  def evalAsync[T](script: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) =
    r.send(Eval(script, kvs.map{kv => kv._1 -> BinaryConverter.StringConverter.write(kv._2)} : _*)).map(multiBulkDataResultToSet(conv))

  def eval[T](script: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) = await { evalAsync(script, kvs: _*) }

  def evalshaAsync[T](script: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) =
    r.send(EvalSha(script, kvs.map{kv => kv._1 -> BinaryConverter.StringConverter.write(kv._2)} : _*)).map(multiBulkDataResultToSet(conv))

  def evalsha[T](digest: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) = await { evalshaAsync(digest, kvs: _*) }

  def await[T](f: Future[T]) = Await.result[T](f, timeout)
}
