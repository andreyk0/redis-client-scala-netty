package com.fotolog.redis

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

sealed abstract class KeyType
object KeyType {
	def apply(s: String): KeyType = {
		s match {
			case "none" => None
			case "string" => String
			case "list" => List
			case "set" => Set
			case "zset" => Zset
			case "hash" => Hash
		}
	}

	case object None extends KeyType // key does not exist
	case object String extends KeyType // binary String value, any seq of bytes 
	case object List extends KeyType // contains a List value
	case object Set extends KeyType // contains a Set value
	case object Zset extends KeyType // contains a Sorted Set value
	case object Hash extends KeyType // contains a Hash value
}

object Conversions {
	import RedisClientTypes._

//	implicit def getValueFromTheFuture[T](f: Future[T]): T = f.get()
//	implicit def convertProtobufToByteArray(m: Message): BinVal = m.toByteArray

	implicit def convertStringToByteArray(s: String): BinVal = s.getBytes
	implicit def convertByteArrayToString(b: BinVal): String = new String(b)
}

object RedisClient {
	import RedisClientTypes._

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
			case Some(data) => Some(convert(data))
		}
	}
	
	private[redis] def multiBulkDataResultToFilteredSeq[T](convert: (BinVal)=>T): PartialFunction[Result, Seq[T]] = {
		case MultiBulkDataResult(results) => results.filter{ r => r match {
				case BulkDataResult(Some(_)) => true
				case BulkDataResult(None) => false
		}}.map{ r => convert(r.data.get)}
	}

	private[redis] def multiBulkDataResultToMap[T](keys: Seq[String], convert: (BinVal)=>T): PartialFunction[Result, Map[String,T]] = {
		case BulkDataResult(data) => data match {
			case None => Map()
			case Some(data) => Map(keys.head -> convert(data))
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
	import RedisClientTypes._
	import RedisClient.integerResultAsBoolean
	import RedisClient.integerResultAsInt
	import RedisClient.okResultAsBoolean
	import RedisClient.bulkDataResultToOpt
	import RedisClient.multiBulkDataResultToFilteredSeq
	import RedisClient.multiBulkDataResultToMap
	import Conversions._

	def isConnected(): Boolean = r.isOpen
	def shutdown() { r.shutdown }
	
	def flushall = r(FlushAll()).get

	def ping(): Boolean = ClientFuture(r(Ping())){
		case SingleLineResult("PONG") => true
	}.get
	
	def info(): Map[String,String] = ClientFuture(r(Info())){
		case BulkDataResult(Some(data)) => {
			val info = convertByteArrayToString(data)
			Map.empty[String,String] ++ info.split("\r\n").map{_.split(":")}.map{x=>x(0)->x(1)}
		}
	}.get
	
	def keytypeAsync(key: String): Future[KeyType] = ClientFuture(r(Type(key))){
		case SingleLineResult(s) => KeyType(s)
	}
	def keytype(key: String): KeyType = keytypeAsync(key).get

	def existsAsync(key: String): Future[Boolean] = ClientFuture(r(Exists(key)))(integerResultAsBoolean)
	def exists(key: String): Boolean = existsAsync(key).get
	def ? (key: String): Boolean = exists(key)
	
	def setAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(Set(key -> convert(value))))(okResultAsBoolean)
	def set[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = setAsync(key, value)(convert).get
	def setAsync[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(Set(kvs.map{kv => kv._1 -> convert(kv._2)} : _*)))(okResultAsBoolean)
	def set[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = setAsync(kvs: _*)(convert).get
	def setAsync[T](key: String, expiration: Int, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(SetEx(key, expiration, convert(value))))(okResultAsBoolean)
	def set[T](key: String, expiration: Int, value: T)(implicit convert: (T)=>BinVal): Boolean = setAsync(key, expiration, value)(convert).get
	def +[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = set(key, value)(convert)
	def +[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = set(kvs: _*)(convert)
	def ++[T](kvs: TraversableOnce[(String,T)])(implicit convert: (T)=>BinVal): Boolean = set(kvs.toSeq: _*)(convert)

	def delAsync(key: String): Future[Boolean] = ClientFuture(r(Del(key)))(integerResultAsBoolean)
	def del(key: String): Boolean = delAsync(key).get
	def -(key: String): Boolean = del(key)

	def getAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(Get(key)))(bulkDataResultToOpt(convert))
	def getAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[Seq[Option[T]]] = ClientFuture(r(Get(keys: _*))) {
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
	def get[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = getAsync(key)(convert).get
	def get[T](keys: String*)(implicit convert: (BinVal)=>T): Seq[Option[T]] = getAsync(keys: _*)(convert).get
	def apply[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = get(key)(convert)

	def mgetAsync[T](keys: String*)(implicit convert: (BinVal)=>T): Future[Map[String,T]] = ClientFuture(r(Get(keys: _*)))(multiBulkDataResultToMap(keys,convert))
	def mget[T](keys: String*)(implicit convert: (BinVal)=>T): Map[String,T] = mgetAsync(keys: _*)(convert).get
	def apply[T](keys: String*)(implicit convert: (BinVal)=>T): Map[String,T] = mget(keys: _*)(convert)
	def apply[T](keys: TraversableOnce[String])(implicit convert: (BinVal)=>T): Map[String,T] = mget(keys.toSeq: _*)(convert)
	
	def setnxAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(SetNx(key -> convert(value))))(integerResultAsBoolean)
	def setnxAsync[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(SetNx(kvs.map{kv => kv._1 -> convert(kv._2)} : _*)))(integerResultAsBoolean)
	def setnx[T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = setnxAsync(key, value)(convert).get
	def setnx[T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = setnxAsync(kvs: _*)(convert).get
	def +? [T](key: String, value: T)(implicit convert: (T)=>BinVal): Boolean = setnx(key, value)(convert)
	def +? [T](kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = setnx(kvs: _*)(convert)

	def getsetAsync[T](key: String, value: T)(implicit convertTo: (T)=>BinVal, convertFrom: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(GetSet(key -> convertTo(value))))(bulkDataResultToOpt(convertFrom))
	def getset[T](key: String, value: T)(implicit convertTo: (T)=>BinVal, convertFrom: (BinVal)=>T): Option[T] = getsetAsync(key, value)(convertTo, convertFrom).get

	def incrAsync[T](key: String, delta: Int = 1): Future[Int] = ClientFuture(r(Incr(key, delta)))(integerResultAsInt)
	def incr[T](key: String, delta: Int = 1): Int = incrAsync(key, delta).get

	def decrAsync[T](key: String, delta: Int = 1): Future[Int] = ClientFuture(r(Decr(key, delta)))(integerResultAsInt)
	def decr[T](key: String, delta: Int = 1): Int = decrAsync(key, delta).get
	
	def appendAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Int] = ClientFuture(r(Append(key -> convert(value))))(integerResultAsInt)
	def append[T](key: String, value: T)(implicit convert: (T)=>BinVal): Int = appendAsync(key, value)(convert).get

	def substrAsync[T](key: String, startOffset: Int, endOffset: Int)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(Substr(key, startOffset, endOffset)))(bulkDataResultToOpt(convert))
	def substr[T](key: String, startOffset: Int, endOffset: Int)(implicit convert: (BinVal)=>T): Option[T] = substrAsync(key, startOffset, endOffset)(convert).get
	
	def expireAsync(key: String, seconds: Int): Future[Boolean] = ClientFuture(r(Expire(key, seconds)))(integerResultAsBoolean)
	def expire(key: String, seconds: Int): Boolean = expireAsync(key, seconds).get

	def persistAsync(key: String): Future[Boolean] = ClientFuture(r(Persist(key)))(integerResultAsBoolean)
	def persist(key: String): Boolean = persistAsync(key).get

	def rpushAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Int] =  ClientFuture(r(Rpush(key, convert(value))))(integerResultAsInt)
	def rpush[T](key: String, value: T)(implicit convert: (T)=>BinVal): Int = rpushAsync(key, value).get

	def lpushAsync[T](key: String, value: T)(implicit convert: (T)=>BinVal): Future[Int] = ClientFuture(r(Lpush(key, convert(value))))(integerResultAsInt)
	def lpush[T](key: String, value: T)(implicit convert: (T)=>BinVal): Int = lpushAsync(key, value).get

	def llenAsync[T](key: String): Future[Int] =  ClientFuture(r(Llen(key)))(integerResultAsInt)
	def llen[T](key: String): Int = llenAsync(key).get  

	def lrangeAsync[T](key: String, start: Int, end: Int)(implicit convert: (BinVal)=>T): Future[Seq[T]] = ClientFuture(r(Lrange(key, start, end)))(multiBulkDataResultToFilteredSeq(convert))
	def lrange[T](key: String, start: Int, end: Int)(implicit convert: (BinVal)=>T): Seq[T] = lrangeAsync(key, start, end)(convert).get

	def ltrimAsync(key: String, start: Int, end: Int): Future[Boolean] = ClientFuture(r(Ltrim(key, start, end)))(okResultAsBoolean)
	def ltrim(key: String, start: Int, end: Int): Boolean = ltrimAsync(key, start, end).get

	def lindexAsync[T](key: String, idx: Int)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(Lindex(key, idx)))(bulkDataResultToOpt(convert))		
	def lindex[T](key: String, idx: Int)(implicit convert: (BinVal)=>T): Option[T] = lindexAsync(key, idx)(convert).get

	def lsetAsync[T](key: String, idx: Int, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(Lset(key, idx, convert(value))))(okResultAsBoolean)
	def lset[T](key: String, idx: Int, value: T)(implicit convert: (T)=>BinVal): Boolean = lsetAsync(key, idx, value)(convert).get

	def lremAsync[T](key: String, count: Int, value: T)(implicit convert: (T)=>BinVal): Future[Int] = ClientFuture(r(Lrem(key, count, convert(value))))(integerResultAsInt)
	def lrem[T](key: String, count: Int, value: T)(implicit convert: (T)=>BinVal): Int = lremAsync(key, count, value)(convert).get

	def lpopAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(Lpop(key)))(bulkDataResultToOpt(convert))		
	def lpop[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = lpopAsync(key)(convert).get

	def rpopAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(Rpop(key)))(bulkDataResultToOpt(convert))		
	def rpop[T](key: String)(implicit convert: (BinVal)=>T): Option[T] = rpopAsync(key)(convert).get

	def rpoplpushAsync[T](srcKey: String, destKey: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(RpopLpush(srcKey, destKey)))(bulkDataResultToOpt(convert))
	def rpoplpush[T](srcKey: String, destKey: String)(implicit convert: (BinVal)=>T): Option[T] = rpoplpushAsync(srcKey, destKey)(convert).get

	def hsetAsync[T](key: String, field: String, value: T)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(Hset(key, field, convert(value))))(integerResultAsBoolean)
	def hset[T](key: String, field: String, value: T)(implicit convert: (T)=>BinVal): Boolean = hsetAsync(key, field, value)(convert).get

	def hgetAsync[T](key: String, field: String)(implicit convert: (BinVal)=>T): Future[Option[T]] = ClientFuture(r(Hget(key, field)))(bulkDataResultToOpt(convert))
	def hget[T](key: String, field: String)(implicit convert: (BinVal)=>T): Option[T] = hgetAsync(key, field)(convert).get

	def hmgetAsync[T](key: String, fields: String*)(implicit convert: (BinVal)=>T): Future[Map[String,T]] = ClientFuture(r(Hmget(key, fields: _*)))(multiBulkDataResultToMap(fields, convert))
	def hmget[T](key: String, fields: String*)(implicit convert: (BinVal)=>T): Map[String,T] = hmgetAsync(key, fields: _*)(convert).get 

	def hmsetAsync[T](key: String, kvs: (String,T)*)(implicit convert: (T)=>BinVal): Future[Boolean] = ClientFuture(r(Hmset(key, kvs.map{kv => kv._1 -> convert(kv._2)} : _*)))(okResultAsBoolean)
	def hmset[T](key: String, kvs: (String,T)*)(implicit convert: (T)=>BinVal): Boolean = hmsetAsync(key, kvs: _*)(convert).get

	def hincrAsync(key: String, field: String, delta: Int = 1): Future[Int] = ClientFuture(r(Hincrby(key, field, delta)))(integerResultAsInt)
	def hincr(key: String, field: String, delta: Int = 1): Int = hincrAsync(key, field, delta).get

	def hexistsAsync(key: String, field: String): Future[Boolean] = ClientFuture(r(Hexists(key, field)))(integerResultAsBoolean)
	def hexists(key: String, field: String): Boolean = hexistsAsync(key, field).get

	def hdelAsync(key: String, field: String): Future[Boolean] = ClientFuture(r(Hdel(key, field)))(integerResultAsBoolean)
	def hdel(key: String, field: String): Boolean = hdelAsync(key, field).get

	def hlenAsync(key: String): Future[Int] = ClientFuture(r(Hlen(key)))(integerResultAsInt)
	def hlen(key: String): Int = hlenAsync(key).get

	def hkeysAsync(key: String): Future[Seq[String]] = ClientFuture(r(Hkeys(key)))(multiBulkDataResultToFilteredSeq(convertByteArrayToString))
	def hkeys(key: String): Seq[String] = hkeysAsync(key).get

	def hvalsAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Seq[T]] = ClientFuture(r(Hvals(key)))(multiBulkDataResultToFilteredSeq(convert))
	def hvals[T](key: String)(implicit convert: (BinVal)=>T): Seq[T] = hvalsAsync(key)(convert).get
	
	def hgetallAsync[T](key: String)(implicit convert: (BinVal)=>T): Future[Map[String,T]] = ClientFuture(r(Hgetall(key))) {
		case MultiBulkDataResult(List()) => Map.empty[String,T]
		case MultiBulkDataResult(results) => Map.empty[String, T] ++ {
			var take = false
			results.zip(results.tail).filter{ (_) => take = !take; take }.filter{ kv => kv match {
				case (k, BulkDataResult(Some(_))) => true
				case (k, BulkDataResult(None)) => false
			}}.map{ kv => convertByteArrayToString(kv._1.data.get) -> convert(kv._2.data.get)}
		}
	}
	def hgetall[T](key: String)(implicit convert: (BinVal)=>T): Map[String,T] = hgetallAsync(key)(convert).get
}

object ClientFuture {
	def apply[T](redisResult: ResultFuture)(resultConverter: PartialFunction[Result, T]) = new ClientFuture(redisResult, resultConverter)
}

class ClientFuture[T](redisResult: ResultFuture, resultConverter: PartialFunction[Result, T]) extends Future[T] {
	override def get(): T = get(10, TimeUnit.SECONDS)
	override def isCancelled(): Boolean = redisResult.isCancelled
	override def cancel(p: Boolean): Boolean = redisResult.cancel(p)
	override def isDone(): Boolean = redisResult.isDone
	override def get(t: Long, unit: TimeUnit): T = {
		val rr = redisResult.get(t, unit)
		rr match {
			case ErrorResult(err) => throw new RedisClientException(err)
			case x => resultConverter(x)
		}
	}
}

class RedisClientException(msg: String) extends RuntimeException(msg)
