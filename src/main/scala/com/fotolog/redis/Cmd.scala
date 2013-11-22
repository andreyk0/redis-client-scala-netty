package com.fotolog.redis

import RedisClientTypes._
import java.nio.charset.Charset

private[redis] object Cmd {

  val charset = Charset.forName("UTF-8")

  val SPACE = " ".getBytes
  val EOL = "\r\n".getBytes

  val DEL = "DEL".getBytes
  val GET = "GET".getBytes
  val MGET = "MGET".getBytes
  val SET = "SET".getBytes
  val MSET = "MSET".getBytes
  val GETSET = "GETSET".getBytes
  val SETNX = "SETNX".getBytes
  val MSETNX = "MSETNX".getBytes
  val SETEX = "SETEX".getBytes
  val INCR = "INCR".getBytes
  val INCRBY = "INCRBY".getBytes
  val DECR = "DECR".getBytes
  val DECRBY = "DECRBY".getBytes
  val APPEND = "APPEND".getBytes
  val SUBSTR = "SUBSTR".getBytes
  val EXPIRE = "EXPIRE".getBytes
  val PERSIST = "PERSIST".getBytes
  val RPUSH = "RPUSH".getBytes
  val LPUSH = "LPUSH".getBytes
  val LLEN = "LLEN".getBytes
  val LRANGE = "LRANGE".getBytes
  val LTRIM = "LTRIM".getBytes
  val LINDEX = "LINDEX".getBytes
  val LSET = "LSET".getBytes
  val LREM = "LREM".getBytes
  val LPOP = "LPOP".getBytes
  val RPOP = "RPOP".getBytes
  val BLPOP = "BLPOP".getBytes
  val BRPOP = "BRPOP".getBytes
  val RPOPLPUSH = "RPOPLPUSH".getBytes
  val HSET = "HSET".getBytes
  val HGET = "HGET".getBytes
  val HMGET = "HMGET".getBytes
  val HMSET = "HMSET".getBytes
  val HINCRBY = "HINCRBY".getBytes
  val HEXISTS    = "HEXISTS".getBytes
  val HDEL = "HDEL".getBytes
  val HLEN = "HLEN".getBytes
  val HKEYS = "HKEYS".getBytes
  val HVALS = "HVALS".getBytes
  val HGETALL = "HGETALL".getBytes
  val SADD = "SADD".getBytes
  val SREM = "SREM".getBytes
  val SPOP = "SPOP".getBytes
  val SMOVE = "SMOVE".getBytes
  val SCARD = "SCARD".getBytes
  val SISMEMBER = "SISMEMBER".getBytes
  val SINTER = "SINTER".getBytes
  val SINTERSTORE = "SINTERSTORE".getBytes
  val SUNION = "SUNION".getBytes
  val SUNIONSTORE = "SUNIONSTORE".getBytes
  val SDIFF = "SDIFF".getBytes
  val SDIFFSTORE = "SDIFFSTORE".getBytes
  val SMEMBERS = "SMEMBERS".getBytes
  val SRANDMEMBER = "SRANDMEMBER".getBytes
  val SORT = "SORT".getBytes

  val EVAL = "EVAL".getBytes
  val EVALSHA = "EVALSHA".getBytes

  val SCRIPT = "SCRIPT".getBytes
  val SCRIPT_LOAD = "LOAD".getBytes
  val SCRIPT_FLUSH = "FLUSH".getBytes
  val SCRIPT_KILL = "KILL".getBytes
  val SCRIPT_EXISTS = "EXISTS".getBytes

  val PING = "PING".getBytes
  val EXISTS = "EXISTS".getBytes
  val TYPE = "TYPE".getBytes
  val INFO = "INFO".getBytes
  val FLUSHALL = "FLUSHALL".getBytes
}

import Cmd._
sealed abstract class Cmd {
  def asBin: Seq[BinVal]
}

case class Exists(key: String) extends Cmd {
  def asBin = Seq(EXISTS, key.getBytes(charset))
}

case class Type(key: String) extends Cmd {
  def asBin = Seq(TYPE, key.getBytes(charset))
}

case class Del(keys: String*) extends Cmd {
  def asBin = if(keys.length > 1)
    DEL :: keys.toList.map(_.getBytes(charset))
  else Seq(DEL, keys.head.getBytes(charset))
}

case class Get(key: String) extends Cmd {
  def asBin = Seq(GET, key.getBytes(charset))
}

case class MGet(keys: String*) extends Cmd {
  def asBin = MGET :: keys.toList.map(_.getBytes(charset))
}

case class Set(key: String, v: BinVal) extends Cmd {
  def asBin = Seq(SET, key.getBytes(charset), v)
}

case class MSet(kvs: KV*) extends Cmd {
  def asBin = MSET :: kvs.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten
}

case class SetNx(kvs: KV*) extends Cmd {
  def asBin = MSETNX :: kvs.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten
}

case class GetSet(kv: KV) extends Cmd {
  def asBin = Seq(GETSET, kv._1.getBytes(charset), kv._2)
}

case class SetEx(key: String, expTime: Int, value: BinVal) extends Cmd {
  def asBin = Seq(SETEX, key.getBytes(charset), expTime.toString.getBytes, value)
}

case class Incr(key: String, delta: Int = 1) extends Cmd {
  def asBin = if(delta == 1) Seq(INCR, key.getBytes(charset))
    else Seq(INCRBY, key.getBytes(charset), delta.toString.getBytes)
}

case class Decr(key: String, delta: Int = 1) extends Cmd {
  def asBin = if(delta == 1) Seq(DECR, key.getBytes(charset))
    else Seq(DECRBY, key.getBytes(charset), delta.toString.getBytes)
}

case class Append(kv: KV) extends Cmd {
  def asBin = Seq(APPEND, kv._1.getBytes(charset), kv._2)
}

case class Substr(key: String, startOffset: Int, endOffset: Int) extends Cmd {
  def asBin = Seq(SUBSTR, key.getBytes(charset), startOffset.toString.getBytes, endOffset.toString.getBytes)
}

case class Expire(key: String, seconds: Int) extends Cmd {
  def asBin = Seq(EXPIRE, key.getBytes(charset), seconds.toString.getBytes(charset))
}

case class Persist(key: String) extends Cmd {
  def asBin = Seq(PERSIST, key.getBytes(charset))
}

// lists
case class Rpush(kv: KV) extends Cmd {
  def asBin = Seq(RPUSH, kv._1.getBytes(charset), kv._2)
}

case class Lpush(kv: KV) extends Cmd {
  def asBin = Seq(LPUSH, kv._1.getBytes(charset), kv._2)
}

case class Llen(key: String) extends Cmd {
  def asBin = Seq(LLEN, key.getBytes(charset))
}

case class Lrange(key: String, start: Int, end: Int) extends Cmd {
  def asBin = Seq(LRANGE, key.getBytes(charset), start.toString.getBytes, end.toString.getBytes)
}

case class Ltrim(key: String, start: Int, end: Int) extends Cmd {
  def asBin = Seq(LTRIM, key.getBytes(charset), start.toString.getBytes, end.toString.getBytes)
}

case class Lindex(key: String, idx: Int) extends Cmd {
  def asBin = Seq(LINDEX, key.getBytes(charset), idx.toString.getBytes)
}

case class Lset(key: String, idx: Int, value: BinVal) extends Cmd {
  def asBin = Seq(LSET, key.getBytes(charset), idx.toString.getBytes, value)
}

case class Lrem(key: String, count: Int, value: BinVal) extends Cmd {
  def asBin = Seq(LREM, key.getBytes(charset), count.toString.getBytes, value)
}

case class Lpop(key: String) extends Cmd {
  def asBin = Seq(LPOP, key.getBytes(charset))
}
case class Rpop(key: String) extends Cmd {
  def asBin = Seq(RPOP, key.getBytes(charset))
}

case class RpopLpush(srcKey: String, destKey: String) extends Cmd {
  def asBin = Seq(RPOPLPUSH, srcKey.getBytes(charset), destKey.getBytes(charset))
}

// hashes
case class Hset(key: String, field: String, value: BinVal) extends Cmd {
  def asBin = Seq(HSET, key.getBytes(charset), field.getBytes(charset), value)
}

case class Hget(key: String, field: String) extends Cmd  {
  def asBin = Seq(HGET, key.getBytes(charset), field.getBytes(charset))
}

case class Hmget(key: String, fields: String*) extends Cmd {
  def asBin = Seq(HMGET :: key.getBytes(charset) :: fields.toList.map{_.getBytes(charset)}: _*)
}

case class Hmset(key:String, kvs: KV*) extends Cmd {
  def asBin = Seq(HMSET :: key.getBytes :: kvs.toList.map{kv => List(kv._1.getBytes(charset), kv._2)}.flatten: _*)
}

case class Hincrby(key: String, field: String, delta: Int) extends Cmd {
  def asBin = Seq(HINCRBY, key.getBytes(charset), field.getBytes(charset), delta.toString.getBytes)
}

case class Hexists(key: String, field: String) extends Cmd {
  def asBin = Seq(HEXISTS, key.getBytes(charset), field.getBytes(charset))
}

case class Hdel(key: String, field: String) extends Cmd {
  def asBin = Seq(HDEL, key.getBytes(charset), field.getBytes(charset))
}

case class Hlen(key: String) extends Cmd {
  def asBin = Seq(HLEN, key.getBytes(charset))
}

case class Hkeys(key: String) extends Cmd {
  def asBin = Seq(HKEYS, key.getBytes(charset))
}

case class Hvals(key: String) extends Cmd {
  def asBin = Seq(HVALS, key.getBytes(charset))
}

case class Hgetall(key: String) extends Cmd {
  def asBin = Seq(HGETALL, key.getBytes(charset))
}

// sets
case class Sadd(kv: KV) extends Cmd {
  def asBin = Seq(SADD, kv._1.getBytes(charset), kv._2)
}

case class Srem(kv: KV) extends Cmd {
  def asBin = Seq(SREM, kv._1.getBytes(charset), kv._2)
}

case class Spop(key: String) extends Cmd {
  def asBin = Seq(SPOP, key.getBytes(charset))
}

case class Smove(srcKey: String, destKey: String, value: BinVal) extends Cmd {
  def asBin = Seq(SMOVE, srcKey.getBytes(charset), destKey.getBytes(charset), value)
}

case class Scard(key: String) extends Cmd {
  def asBin = Seq(SCARD, key.getBytes(charset))
}
case class Sismember(kv: KV) extends Cmd  {
  def asBin = Seq(SISMEMBER, kv._1.getBytes(charset), kv._2)
}

case class Sinter(keys: String*) extends Cmd {
  def asBin = SINTER :: keys.toList.map{_.getBytes(charset)}
}

case class Sinterstore(destKey: String, keys: String*) extends Cmd {
  def asBin = SINTERSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
}

case class Sunion(keys: String*) extends Cmd {
  def asBin = SUNION :: keys.toList.map(_.getBytes(charset))
}

case class Sunionstore(destKey: String, keys: String*) extends Cmd {
  def asBin = SUNIONSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
}

case class Sdiff(keys: String*) extends Cmd {
  def asBin = SDIFF :: keys.toList.map{_.getBytes(charset)}
}

case class Sdiffstore(destKey: String, keys: String*) extends Cmd {
  def asBin = SDIFFSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
}

case class Smembers(key: String) extends Cmd {
  def asBin = Seq(SMEMBERS, key.getBytes(charset))
}

case class Srandmember(key: String) extends Cmd {
  def asBin = Seq(SRANDMEMBER, key.getBytes(charset))
}

// scripting
case class Eval(script: String, kv: KV*) extends Cmd {
  def asBin = EVAL :: script.getBytes(charset) :: kv.length.toString.getBytes :: kv.toList.map{ kv => List(kv._1.getBytes(charset), kv._2)}.flatten
}

case class EvalSha(digest: String, kv: KV*) extends Cmd {
  def asBin = EVALSHA :: digest.getBytes(charset) :: kv.length.toString.getBytes :: kv.toList.map{ kv => List(kv._1.getBytes(charset), kv._2)}.flatten
}

case class ScriptLoad(script: String) extends Cmd {
  def asBin = Seq(SCRIPT, SCRIPT_LOAD, script.getBytes(charset))
}

case class ScriptKill() extends Cmd { def asBin = Seq(SCRIPT, SCRIPT_KILL) }
case class ScriptFlush() extends Cmd { def asBin = Seq(SCRIPT, SCRIPT_FLUSH) }
case class ScriptExists(script: String) extends Cmd { def asBin = Seq(SCRIPT, SCRIPT_EXISTS, script.getBytes(charset)) }

// utils
case class Ping() extends Cmd { def asBin = Seq(PING) }
case class Info() extends Cmd { def asBin = Seq(INFO) }
case class FlushAll() extends Cmd { def asBin = Seq(FLUSHALL) }