package com.fotolog.redis

import RedisClientTypes._
import RedisCommandEncoder._

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
  def asBin =
    Seq(GET, key.getBytes(charset))
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
  def asBin = Seq(SETEX, key.getBytes(charset), expTime.toString.getBytes(charset), value)
}

case class Incr(key: String, delta: Int = 1) extends Cmd {
  def asBin = if(delta == 1) Seq(INCR, key.getBytes(charset))
    else Seq(INCRBY, key.getBytes(charset), delta.toString.getBytes(charset))
}

case class Decr(key: String, delta: Int = 1) extends Cmd {
  def asBin = if(delta == 1) Seq(DECR, key.getBytes(charset))
    else Seq(DECRBY, key.getBytes(charset), delta.toString.getBytes(charset))
}

case class Append(kv: KV) extends Cmd {
  def asBin = Seq(APPEND, kv._1.getBytes(charset), kv._2)
}

case class Substr(key: String, startOffset: Int, endOffset: Int) extends Cmd {
  def asBin = Seq(SUBSTR, key.getBytes(charset), startOffset.toString.getBytes(charset), endOffset.toString.getBytes(charset))
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
  def asBin = Seq(LRANGE, key.getBytes(charset), start.toString.getBytes(charset), end.toString.getBytes(charset))
}

case class Ltrim(key: String, start: Int, end: Int) extends Cmd {
  def asBin = Seq(LTRIM, key.getBytes(charset), start.toString.getBytes(charset), end.toString.getBytes(charset))
}

case class Lindex(key: String, idx: Int) extends Cmd {
  def asBin = Seq(LINDEX, key.getBytes(charset), idx.toString.getBytes(charset))
}

case class Lset(key: String, idx: Int, value: BinVal) extends Cmd {
  def asBin = Seq(LSET, key.getBytes(charset), idx.toString.getBytes(charset), value)
}

case class Lrem(key: String, count: Int, value: BinVal) extends Cmd {
  def asBin = Seq(LREM, key.getBytes(charset), count.toString.getBytes(charset), value)
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
  def asBin = Seq(HINCRBY, key.getBytes(charset), field.getBytes(charset), delta.toString.getBytes(charset))
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
  def asBin = SUNION:: keys.toList.map{_.getBytes(charset)}
}

case class Sunionstore(destKey: String, keys: String*) extends Cmd {
  def asBin = SUNIONSTORE:: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
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
  def asBin = Seq(SCRIPT_LOAD, script.getBytes(charset))
}

case class ScriptKill() extends Cmd { def asBin = Seq(SCRIPT_KILL) }
case class ScriptFlush() extends Cmd { def asBin = Seq(SCRIPT_FLUSH) }
case class ScriptExists(script: String) extends Cmd { def asBin = Seq(SCRIPT_EXISTS, script.getBytes(charset)) }

// utils
case class Ping() extends Cmd { def asBin = Seq(PING) }
case class Info() extends Cmd { def asBin = Seq(INFO) }
case class FlushAll() extends Cmd { def asBin = Seq(FLUSHALL) }