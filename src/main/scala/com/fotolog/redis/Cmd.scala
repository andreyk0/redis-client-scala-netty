package com.fotolog.redis

import RedisClientTypes._

// tested with 2.1.3 redis, ubuntu 10.04 ships with 1.2.0 where some newer commands won't work
sealed abstract class Cmd
case class Exists(key: String) extends Cmd
case class Type(key: String) extends Cmd
case class Del(key: String*) extends Cmd
case class Get(key: String) extends Cmd
case class MGet(key: String*) extends Cmd
case class Set(key: String, v: BinVal) extends Cmd
case class MSet(kvs: KV*) extends Cmd
case class SetNx(kvs: KV*) extends Cmd
case class GetSet(kv: KV) extends Cmd
case class SetEx(key: String, expTime: Int, value: BinVal) extends Cmd
case class Incr(key: String, delta: Int = 1) extends Cmd
case class Decr(key: String, delta: Int = 1) extends Cmd
case class Append(kv: KV) extends Cmd
case class Substr(key: String, startOffset: Int, endOffset: Int) extends Cmd
case class Expire(key: String, seconds: Int) extends Cmd
case class Persist(key: String) extends Cmd
// lists
case class Rpush(kv: KV) extends Cmd
case class Lpush(kv: KV) extends Cmd
case class Llen(key: String) extends Cmd
case class Lrange(key: String, start: Int, end: Int) extends Cmd
case class Ltrim(key: String, start: Int, end: Int) extends Cmd
case class Lindex(key: String, idx: Int) extends Cmd
case class Lset(key: String, idx: Int, value: BinVal) extends Cmd
case class Lrem(key: String, count: Int, value: BinVal) extends Cmd
case class Lpop(key: String) extends Cmd
case class Rpop(key: String) extends Cmd
case class RpopLpush(srcKey: String, destKey: String) extends Cmd
// hashes
case class Hset(key: String, field: String, value: BinVal) extends Cmd
case class Hget(key: String, field: String) extends Cmd
case class Hmget(key: String, fields: String*) extends Cmd
case class Hmset(key:String, kvs: KV*) extends Cmd
case class Hincrby(key: String, field: String, delta: Int) extends Cmd
case class Hexists(key: String, field: String) extends Cmd
case class Hdel(key: String, field: String) extends Cmd
case class Hlen(key: String) extends Cmd
case class Hkeys(key: String) extends Cmd
case class Hvals(key: String) extends Cmd
case class Hgetall(key: String) extends Cmd
// sets
case class Sadd(kv: KV) extends Cmd
case class Srem(kv: KV) extends Cmd
case class Spop(key: String) extends Cmd
case class Smove(srcKey: String, destKey: String, value: BinVal) extends Cmd
case class Scard(key: String) extends Cmd
case class Sismember(kv: KV) extends Cmd
case class Sinter(keys: String*) extends Cmd
case class Sinterstore(destKey: String, keys: String*) extends Cmd
case class Sunion(keys: String*) extends Cmd
case class Sunionstore(destKey: String, keys: String*) extends Cmd
case class Sdiff(keys: String*) extends Cmd
case class Sdiffstore(destKey: String, keys: String*) extends Cmd
case class Smembers(key: String) extends Cmd
case class Srandmember(key: String) extends Cmd
//
case class Ping() extends Cmd
case class Info() extends Cmd
case class FlushAll() extends Cmd