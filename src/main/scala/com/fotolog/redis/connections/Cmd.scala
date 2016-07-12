package com.fotolog.redis.connections

import java.nio.charset.Charset

private[redis] object Cmd {

  val charset = Charset.forName("UTF-8")

  val SPACE = " ".getBytes
  val EOL = "\r\n".getBytes

  val NX = "NX".getBytes
  val XX = "XX".getBytes
  val EX = "EX".getBytes

  // set
  val SADD = "SADD".getBytes
  val SCARD = "SCARD".getBytes
  val SDIFF = "SDIFF".getBytes
  val SDIFFSTORE = "SDIFFSTORE".getBytes
  val SINTER = "SINTER".getBytes
  val SINTERSTORE = "SINTERSTORE".getBytes
  val SISMEMBER = "SISMEMBER".getBytes
  val SMEMBERS = "SMEMBERS".getBytes
  val SMOVE = "SMOVE".getBytes
  val SPOP = "SPOP".getBytes
  val SRANDMEMBER = "SRANDMEMBER".getBytes
  val SREM = "SREM".getBytes
  val SUNION = "SUNION".getBytes
  val SUNIONSTORE = "SUNIONSTORE".getBytes
  val SSCAN = "SSCAN".getBytes

  // hash
  val HDEL = "HDEL".getBytes
  val HEXISTS = "HEXISTS".getBytes
  val HGET = "HGET".getBytes
  val HGETALL = "HGETALL".getBytes
  val HINCRBY = "HINCRBY".getBytes
  val HINCRBYFLOAT = "HINCRBYFLOAT".getBytes
  val HKEYS = "HKEYS".getBytes
  val HLEN = "HLEN".getBytes
  val HMGET = "HMGET".getBytes
  val HMSET = "HMSET".getBytes
  val HSET = "HSET".getBytes
  val HSETNX = "HSETNX".getBytes
  val HVALS = "HVALS".getBytes
  val HSCAN = "HSCAN".getBytes
  val HSTRLEN = "HSTRLEN".getBytes

  // string
  val APPEND = "APPEND".getBytes
  val BITCOUNT = "BITCOUNT".getBytes
  val BITOP = "BITOP".getBytes
  val BITPOS = "BITPOS".getBytes
  val DECR = "DECR".getBytes
  val DECRBY = "DECRBY".getBytes
  val GET = "GET".getBytes
  val GETBIT = "GETBIT".getBytes
  val GETRANGE = "GETRANGE".getBytes
  val GETSET = "GETSET".getBytes
  val INCR = "INCR".getBytes
  val INCRBY = "INCRBY".getBytes
  val INCRBYFLOAT = "INCRBYFLOAT".getBytes
  val MGET = "MGET".getBytes
  val MSET = "MSET".getBytes
  val MSETNX = "MSETNX".getBytes
  val PSETEX = "PSETEX".getBytes
  val SET = "SET".getBytes
  val SETBIT = "SETBIT".getBytes
  val SETEX = "SETEX".getBytes
  val SETNX = "SETNX".getBytes
  val SETRANGE = "SETRANGE".getBytes
  val STRLEN = "STRLEN".getBytes

  // transactions
  val DISCARD = "DISCARD".getBytes
  val EXEC = "EXEC".getBytes
  val MULTI = "MULTI".getBytes
  val UNWATCH = "UNWATCH".getBytes
  val WATCH = "WATCH".getBytes

  // generic
  val DEL = "DEL".getBytes
  val DUMP = "DUMP".getBytes // not used
  val EXISTS = "EXISTS".getBytes
  val EXPIRE = "EXPIRE".getBytes
  val EXPIREAT = "EXPIREAT".getBytes // not used
  val KEYS = "KEYS".getBytes
  val MIGRATE = "MIGRATE".getBytes // not used
  val MOVE = "MOVE".getBytes // not used
  val OBJECT = "OBJECT".getBytes // not used
  val PERSIST = "PERSIST".getBytes
  val PEXPIRE = "PEXPIRE".getBytes // not used
  val PEXPIREAT = "PEXPIREAT".getBytes // not used
  val PTTL = "PTTL".getBytes // not used
  val RANDOMKEY = "RANDOMKEY".getBytes // not used
  val RENAME = "RENAME".getBytes
  val RENAMENX = "RENAMENX".getBytes
  val RESTORE = "RESTORE".getBytes // not used
  val SCAN = "SCAN".getBytes // TODO:
  val SORT = "SORT".getBytes
  val TTL = "TTL".getBytes
  val TYPE = "TYPE".getBytes
  val AUTH = "AUTH".getBytes

  // list
  val BLPOP = "BLPOP".getBytes
  val BRPOP = "BRPOP".getBytes
  val BRPOPLPUSH = "BRPOPLPUSH".getBytes
  val LINDEX = "LINDEX".getBytes
  val LINSERT = "LINSERT".getBytes
  val LLEN = "LLEN".getBytes
  val LPOP = "LPOP".getBytes
  val LPUSH = "LPUSH".getBytes
  val LPUSHX = "LPUSHX".getBytes
  val LRANGE = "LRANGE".getBytes
  val LREM = "LREM".getBytes
  val LSET = "LSET".getBytes
  val LTRIM = "LTRIM".getBytes
  val RPOP = "RPOP".getBytes
  val RPOPLPUSH = "RPOPLPUSH".getBytes
  val RPUSH = "RPUSH".getBytes
  val RPUSHX = "RPUSHX".getBytes

  // server
  val BGREWRITEAOF = "BGREWRITEAOF".getBytes
  val BGSAVE = "BGSAVE".getBytes
  val CLIENT_KILL = Seq("CLIENT".getBytes, "KILL".getBytes)
  val CLIENT_LIST = Seq("CLIENT".getBytes, "LIST".getBytes)
  val CLIENT_GETNAME = Seq("CLIENT".getBytes, "GETNAME".getBytes)
  val CLIENT_PAUSE = Seq("CLIENT".getBytes, "PAUSE".getBytes)
  val CLIENT_SETNAME = Seq("CLIENT".getBytes, "SETNAME".getBytes)
  val CLUSTER_SLOTS = Seq("CLUSTER".getBytes, "SLOTS".getBytes)
  val COMMAND = "COMMAND".getBytes
  val COMMAND_COUNT = Seq("COMMAND".getBytes, "COUNT".getBytes)
  val COMMAND_GETKEYS = Seq("COMMAND".getBytes, "GETKEYS".getBytes)
  val COMMAND_INFO = Seq("COMMAND".getBytes, "INFO".getBytes)
  val CONFIG_GET = Seq("CONFIG".getBytes, "GET".getBytes)
  val CONFIG_REWRITE = Seq("CONFIG".getBytes, "REWRITE".getBytes)
  val CONFIG_SET = Seq("CONFIG".getBytes, "SET".getBytes)
  val CONFIG_RESETSTAT = Seq("CONFIG".getBytes, "RESETSTAT".getBytes)
  val DBSIZE = "DBSIZE".getBytes
  val DEBUG_OBJECT = Seq("DEBUG".getBytes, "OBJECT".getBytes)
  val DEBUG_SEGFAULT = Seq("DEBUG".getBytes, "SEGFAULT".getBytes)
  val FLUSHALL = "FLUSHALL".getBytes
  val FLUSHDB = "FLUSHDB".getBytes
  val INFO = "INFO".getBytes
  val LASTSAVE = "LASTSAVE".getBytes
  val MONITOR = "MONITOR".getBytes
  val ROLE = "ROLE".getBytes
  val SAVE = "SAVE".getBytes
  val SHUTDOWN = "SHUTDOWN".getBytes
  val SLAVEOF = "SLAVEOF".getBytes
  val SLOWLOG = "SLOWLOG".getBytes
  val SYNC = "SYNC".getBytes
  val TIME = "TIME".getBytes

  // sorted_set
  val ZADD = "ZADD".getBytes
  val ZCARD = "ZCARD".getBytes
  val ZCOUNT = "ZCOUNT".getBytes
  val ZINCRBY = "ZINCRBY".getBytes
  val ZINTERSTORE = "ZINTERSTORE".getBytes
  val ZLEXCOUNT = "ZLEXCOUNT".getBytes
  val ZRANGE = "ZRANGE".getBytes
  val ZRANGEBYLEX = "ZRANGEBYLEX".getBytes
  val ZREVRANGEBYLEX = "ZREVRANGEBYLEX".getBytes
  val ZRANGEBYSCORE = "ZRANGEBYSCORE".getBytes
  val ZRANK = "ZRANK".getBytes
  val ZREM = "ZREM".getBytes
  val ZREMRANGEBYLEX = "ZREMRANGEBYLEX".getBytes
  val ZREMRANGEBYRANK = "ZREMRANGEBYRANK".getBytes
  val ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE".getBytes
  val ZREVRANGE = "ZREVRANGE".getBytes
  val ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE".getBytes
  val ZREVRANK = "ZREVRANK".getBytes
  val ZSCORE = "ZSCORE".getBytes
  val ZUNIONSTORE = "ZUNIONSTORE".getBytes
  val ZSCAN = "ZSCAN".getBytes

  // connection
  val ECHO = "ECHO".getBytes
  val PING = "PING".getBytes
  val QUIT = "QUIT".getBytes
  val SELECT = "SELECT".getBytes

  // pubsub
  val PSUBSCRIBE = "PSUBSCRIBE".getBytes
  val PUBSUB = "PUBSUB".getBytes
  val PUBLISH = "PUBLISH".getBytes
  val PUNSUBSCRIBE = "PUNSUBSCRIBE".getBytes
  val SUBSCRIBE = "SUBSCRIBE".getBytes
  val UNSUBSCRIBE = "UNSUBSCRIBE".getBytes

  // scripting
  val EVAL = "EVAL".getBytes
  val EVALSHA = "EVALSHA".getBytes
  val SCRIPT_EXISTS = Seq("SCRIPT".getBytes, "EXISTS".getBytes)
  val SCRIPT_FLUSH = Seq("SCRIPT".getBytes, "FLUSH".getBytes)
  val SCRIPT_KILL = Seq("SCRIPT".getBytes, "KILL".getBytes)
  val SCRIPT_LOAD = Seq("SCRIPT".getBytes, "LOAD".getBytes)

  // hyperloglog
  val PFADD = "PFADD".getBytes
  val PFCOUNT = "PFCOUNT".getBytes
  val PFMERGE = "PFMERGE".getBytes

  // Geo
  val GEOADD = "GEOADD".getBytes
  val GEODIST = "GEODIST".getBytes
  val GEOHASH = "GEOHASH".getBytes
  val GAORADIUS = "GAORADIUS".getBytes
  val GEOPOS = "GEOPOS".getBytes
  val GEORADIUSBYMEMBER = "GEORADIUSBYMEMBER".getBytes
}

import com.fotolog.redis.connections.Cmd._
sealed abstract class Cmd {
  def asBin: Seq[Array[Byte]]
}

sealed trait ArrayFlatten {
  implicit val flattener2 = (t: (Array[Byte], Array[Byte])) ⇒ t._1.toList ::: t._2.toList

  implicit val flattener3 = (t: (Array[Byte], Array[Byte], Array[Byte])) ⇒ t._1.toList ::: t._2.toList ::: t._3.toList
}

case class Exists(key: String) extends Cmd {
  def asBin = Seq(EXISTS, key.getBytes(charset))
}

case class Type(key: String) extends Cmd {
  def asBin = Seq(TYPE, key.getBytes(charset))
}

case class Del(keys: Seq[String]) extends Cmd {
  def asBin = if(keys.length > 1)
    DEL :: keys.toList.map(_.getBytes(charset))
  else Seq(DEL, keys.head.getBytes(charset))
}

case class Rename(key: String, newKey: String, nx: Boolean) extends Cmd {
  def asBin = Seq(if(nx) RENAMENX else RENAME, key.getBytes(charset), newKey.getBytes(charset))
}

case class Get(key: String) extends Cmd {
  def asBin = Seq(GET, key.getBytes(charset))
}

case class MGet(keys: Seq[String]) extends Cmd {
  def asBin = MGET :: keys.toList.map(_.getBytes(charset))
}

case class SetCmd(key: String,
                  v: Array[Byte],
                  expTime: Int,
                  nx: Boolean = false,
                  xx: Boolean = false) extends Cmd {

  def asBin = {
    var seq = Seq(SET, key.getBytes(charset), v)
    if(expTime != -1) seq = seq ++ Seq(EX, expTime.toString.getBytes)

    if(nx) {
      seq = seq :+ NX
    } else if(xx) {
      seq = seq :+ XX
    }

    seq
  }
}

case class MSet(kvs: Seq[(String, Array[Byte])]) extends Cmd {
  def asBin = MSET :: kvs.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
}

case class SetNx(kvs: Seq[(String, Array[Byte])]) extends Cmd {
  def asBin = MSETNX :: kvs.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
}

case class GetSet(key: String, v: Array[Byte]) extends Cmd {
  def asBin = Seq(GETSET, key.getBytes(charset), v)
}

case class Incr(key: String, delta: Int = 1) extends Cmd {
  def asBin = if(delta == 1) Seq(INCR, key.getBytes(charset))
    else Seq(INCRBY, key.getBytes(charset), delta.toString.getBytes)
}

case class Decr(key: String, delta: Int = 1) extends Cmd {
  def asBin = if(delta == 1) Seq(DECR, key.getBytes(charset))
    else Seq(DECRBY, key.getBytes(charset), delta.toString.getBytes)
}

case class Append(key: String, v: Array[Byte]) extends Cmd {
  def asBin = Seq(APPEND, key.getBytes(charset), v)
}

case class Getrange(key: String, startOffset: Int, endOffset: Int) extends Cmd {
  def asBin = Seq(GETRANGE, key.getBytes(charset), startOffset.toString.getBytes, endOffset.toString.getBytes)
}

case class Expire(key: String, seconds: Int) extends Cmd {
  def asBin = Seq(EXPIRE, key.getBytes(charset), seconds.toString.getBytes(charset))
}

case class Persist(key: String) extends Cmd {
  def asBin = Seq(PERSIST, key.getBytes(charset))
}

case class Ttl(key: String) extends Cmd {
  def asBin = Seq(TTL, key.getBytes(charset))
}

case class Keys(pattern: String) extends Cmd {
  def asBin = Seq(KEYS, pattern.getBytes(charset))
}

// lists
case class Rpush(key: String, v: Array[Byte]) extends Cmd {
  def asBin = Seq(RPUSH, key.getBytes(charset), v)
}

case class Lpush(key: String, v: Array[Byte]) extends Cmd {
  def asBin = Seq(LPUSH, key.getBytes(charset), v)
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

case class Lset(key: String, idx: Int, value: Array[Byte]) extends Cmd {
  def asBin = Seq(LSET, key.getBytes(charset), idx.toString.getBytes, value)
}

case class Lrem(key: String, count: Int, value: Array[Byte]) extends Cmd {
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
case class Hset(key: String, field: String, value: Array[Byte]) extends Cmd {
  def asBin = Seq(HSET, key.getBytes(charset), field.getBytes(charset), value)
}

case class Hget(key: String, field: String) extends Cmd  {
  def asBin = Seq(HGET, key.getBytes(charset), field.getBytes(charset))
}

case class Hmget(key: String, fields: Seq[String]) extends Cmd {
  def asBin = HMGET :: key.getBytes(charset) :: fields.toList.map{_.getBytes(charset)}
}

case class Hmset(key:String, kvs: Seq[(String, Array[Byte])]) extends Cmd {
  def asBin = HMSET :: key.getBytes :: kvs.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
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

case class Hstrlen(key: String, field: String) extends Cmd {
  def asBin = Seq(HSTRLEN, key.getBytes, field.getBytes)
}

case class Hsetnx(key: String, field: String, value: Array[Byte], nx: Boolean = false) extends Cmd {
  def asBin = Seq(if (nx) HSET else HSETNX, key.getBytes(charset), field.getBytes(charset), value)
}

case class Hincrbyfloat(key: String, field: String, delta: Double) extends Cmd {
  def asBin = Seq(HINCRBYFLOAT, key.getBytes(charset), field.getBytes(charset), delta.toString.getBytes)
}

// sets
case class Sadd(key: String, values: Seq[Array[Byte]]) extends Cmd {
  def asBin = Seq(SADD, key.getBytes(charset)) ++ values
}

case class Srem(key: String, v: Array[Byte]) extends Cmd {
  def asBin = Seq(SREM, key.getBytes(charset), v)
}

case class Spop(key: String) extends Cmd {
  def asBin = Seq(SPOP, key.getBytes(charset))
}

case class Smove(srcKey: String, destKey: String, value: Array[Byte]) extends Cmd {
  def asBin = Seq(SMOVE, srcKey.getBytes(charset), destKey.getBytes(charset), value)
}

case class Scard(key: String) extends Cmd {
  def asBin = Seq(SCARD, key.getBytes(charset))
}
case class Sismember(key: String, v: Array[Byte]) extends Cmd  {
  def asBin = Seq(SISMEMBER, key.getBytes(charset), v)
}

case class Sinter(keys: Seq[String]) extends Cmd {
  def asBin = SINTER :: keys.toList.map{_.getBytes(charset)}
}

case class Sinterstore(destKey: String, keys: Seq[String]) extends Cmd {
  def asBin = SINTERSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
}

case class Sunion(keys: Seq[String]) extends Cmd {
  def asBin = SUNION :: keys.toList.map(_.getBytes(charset))
}

case class Sunionstore(destKey: String, keys: Seq[String]) extends Cmd {
  def asBin = SUNIONSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
}

case class Sdiff(keys: Seq[String]) extends Cmd {
  def asBin = SDIFF :: keys.toList.map{_.getBytes(charset)}
}

case class Sdiffstore(destKey: String, keys: Seq[String]) extends Cmd {
  def asBin = SDIFFSTORE :: destKey.getBytes(charset) :: keys.toList.map{_.getBytes(charset)}
}

case class Smembers(key: String) extends Cmd {
  def asBin = Seq(SMEMBERS, key.getBytes(charset))
}

case class Srandmember(key: String) extends Cmd {
  def asBin = Seq(SRANDMEMBER, key.getBytes(charset))
}

// scripting
case class Eval(script: String, kv: Seq[(String, Array[Byte])]) extends Cmd {
  def asBin = EVAL :: script.getBytes(charset) :: kv.length.toString.getBytes :: kv.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
}

case class EvalSha(digest: String, kv: Seq[(String, Array[Byte])]) extends Cmd {
  def asBin = EVALSHA :: digest.getBytes(charset) :: kv.length.toString.getBytes :: kv.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
}

case class ScriptLoad(script: String) extends Cmd { def asBin = SCRIPT_LOAD :+ script.getBytes(charset) }
case class ScriptKill() extends Cmd { def asBin = SCRIPT_KILL }
case class ScriptFlush() extends Cmd { def asBin = SCRIPT_FLUSH }
case class ScriptExists(script: String) extends Cmd { def asBin = SCRIPT_EXISTS :+ script.getBytes(charset) }

// transactioning

case class Multi() extends Cmd { def asBin = Seq(MULTI) }
case class Exec() extends Cmd { def asBin = Seq(EXEC) }
case class Discard() extends Cmd { def asBin = Seq(DISCARD) }
case class Watch(keys: Seq[String])  extends Cmd { def asBin = WATCH :: keys.map(_.getBytes(charset)).toList }
case class Unwatch()  extends Cmd { def asBin = Seq(UNWATCH) }

// pub/sub

case class Publish(channel: String, v: Array[Byte]) extends Cmd {
  def asBin = Seq(PUBLISH, channel.getBytes(charset), v)
}

case class Subscribe(channels: Seq[String], handler: MultiBulkDataResult => Unit) extends Cmd {
  def asBin =
    (if(hasPattern) PSUBSCRIBE else SUBSCRIBE) :: channels.toList.map(_.getBytes(charset))

  def hasPattern = channels.exists(s => s.contains("*") || s.contains("?"))
}

case class Unsubscribe(channels: Seq[String]) extends Cmd {
  def asBin =
    (if(channels.exists(s => s.contains("*") || s.contains("?"))) PUNSUBSCRIBE else UNSUBSCRIBE) :: channels.toList.map(_.getBytes(charset))
}

// hyper log log

case class PfAdd(key: String, values: Seq[Array[Byte]]) extends Cmd {
  def asBin = PFADD :: key.getBytes(charset) :: values.toList
}

case class PfCount(key: String) extends Cmd {
  def asBin = Seq(PFCOUNT, key.getBytes(charset))
}

case class PfMerge(dst: String, keys: Seq[String]) extends Cmd {
  def asBin = PFMERGE :: dst.getBytes(charset) :: keys.toList.map(_.getBytes(charset))
}

// utils
case class Ping() extends Cmd { def asBin = Seq(PING) }
case class Info() extends Cmd { def asBin = Seq(INFO) }
case class FlushAll() extends Cmd { def asBin = Seq(FLUSHALL) }
case class Auth(password: String) extends Cmd { def asBin = Seq(AUTH, password.getBytes(charset)) }
case class Select(db: Int) extends Cmd { def asBin = Seq(SELECT, db.toString.getBytes(charset)) }

// geo

case class GeoAdd2(key: String, values: Seq[(Array[Byte],Array[Byte])]) extends Cmd with ArrayFlatten {
  def asBin = Seq(GEOADD, values.flatten.toArray)
}

case class GeoAdd3(key: String, values: Seq[(Array[Byte],Array[Byte],Array[Byte])]) extends Cmd with ArrayFlatten {
  def asBin = Seq(GEOADD, values.flatten.toArray)
}


case class GeoDist(key: String, member1: String, member2: String, unit: String) extends Cmd {
  def asBin = if ("m".equals(unit)) {
    List(GEODIST, member1.getBytes(charset), member2.getBytes(charset))
  } else {
    List(GEODIST, member1.getBytes(charset), member2.getBytes(charset), unit.getBytes(charset))
  }
}

case class GeoHash(key: String, members: Seq[String]) extends Cmd {
  def asBin = GEOHASH :: key.getBytes(charset) :: members.map(_.getBytes(charset)).toList
}

case class GeoPos(key: String, members: Seq[String]) extends Cmd {
  def asBin = GEOPOS :: key.getBytes(charset) :: members.map(_.getBytes(charset)).toList
}
// TODO: case class GeoRadius extends Cmd { def asBin = GAORADIUS :: Nil }

// TODO: case class GeoRadiusByMember extends Cmd { def asBin = GEORADIUSBYMEMBER :: Nil }
