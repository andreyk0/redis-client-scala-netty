package com.fotolog.redis.connections

import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.fotolog.redis.{RedisException, KeyType}

import scala.collection.JavaConversions._
import scala.compat.Platform
import scala.concurrent.{ExecutionContext, Future}

sealed case class Data(v: AnyRef, ttl: Int = -1, keyType: KeyType = KeyType.String, stamp: Long = Platform.currentTime) {
  def asBytes = keyType match {
    case KeyType.String => v.asInstanceOf[Array[Byte]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def asMap = keyType match {
    case KeyType.Hash => v.asInstanceOf[Map[String, Array[Byte]]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def asSet = keyType match {
    case KeyType.Set => v.asInstanceOf[Set[BytesWrapper]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def expired = ttl != -1 && Platform.currentTime - stamp > (ttl * 1000L)
  def secondsLeft = if (ttl == -1) -1 else (ttl - (Platform.currentTime - stamp) / 1000).toInt
}

private object Data {
  def str(d: Array[Byte], ttl: Int = -1) = Data(d, ttl, keyType = KeyType.String)
  def hash(map: Map[String, Array[Byte]], ttl: Int = -1) = Data(map, ttl, keyType = KeyType.Hash)
  def set(set: Set[BytesWrapper], ttl: Int = -1) = Data(set, ttl, keyType = KeyType.Set)
}

object InMemoryRedisConnection {
  val fakeServers = new ConcurrentHashMap[String, ConcurrentHashMap[String, Data]]

  val context = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

  private[redis] val cleaner = new Runnable {
    override def run() = {
      val servers = fakeServers.elements()

      while(servers.hasMoreElements) {
        val map = servers.nextElement()

        val it = map.entrySet.iterator
        while (it.hasNext) {
          if(it.next.getValue.expired) it.remove()
        }
      }
    }
  }
}

/**
 * Fake redis connection that can be used for testing purposes.
 */
class InMemoryRedisConnection(dbName: String) extends RedisConnection {
  import com.fotolog.redis.connections.InMemoryRedisConnection._
  import com.fotolog.redis.connections.ErrMessages._

  fakeServers.putIfAbsent(dbName, new ConcurrentHashMap[String, Data]())
  val map = fakeServers.get(dbName)

  override def send(cmd: Cmd): Future[Result] = {
    context.execute(cleaner)
    Future { syncSend(cmd) }(context)
  }

  private[this] def syncSend(cmd: Cmd): Result = cmd match {
    case set: SetCmd if set.nx =>
        Option( map.putIfAbsent(set.key, Data(set.v, set.expTime)) ) map(_ => bulkNull) getOrElse ok

    case set: SetCmd if set.xx =>
      Option( map.replace(set.key, Data(set.v, set.expTime)) ) map (_ => ok) getOrElse bulkNull

    case set: SetCmd =>
      map.put(set.key, Data(set.v, set.expTime))
      ok

    case Get(key) =>
      BulkDataResult(
        optVal(key) filterNot(_.expired) map (_.asBytes)
      )

    case Incr(key, delta) =>
      val newVal = (optVal(key) map { a => bytes2int(a.asBytes, ERR_INVALID_NUMBER) } getOrElse 0) + delta
      map.put(key, Data.str(int2bytes(newVal)))
      newVal

    case Keys(pattern) =>
      MultiBulkDataResult(
        map.keys()
           .filter(_.matches(pattern.replace("*", ".*?").replace("?", ".?")))
           .map(k => BulkDataResult(Some(k.getBytes))).toSeq
      )

    case Expire(key, seconds) =>
      int2res(optVal(key) map { d => map.put(key, d.copy(ttl = seconds)); 1 } getOrElse 0)

    case Exists(key) =>
      if(optVal(key).exists(!_.expired)) 1 else 0

    case Type(key) =>
      SingleLineResult(
        optVal(key) map ( _.keyType.name ) getOrElse KeyType.None.name
      )

    case Ttl(key) =>
      int2res(optVal(key) map (_.secondsLeft) getOrElse -2)

    case d: Del =>
      d.keys.count(k => Option(map.remove(k)).isDefined)

    // hash commands

    case hmset: Hmset =>
      map.put(hmset.key, Data.hash(Map(hmset.kvs:_*)))
      ok

    case hmget: Hmget =>
      optVal(hmget.key).map { data =>
        val m = data.asMap
        hmget.fields.map(f => BulkDataResult(m.get(f)) ) match {
          case Seq(one) => one
          case bulks: Seq[BulkDataResult] => MultiBulkDataResult(bulks)
        }
      } getOrElse bulkNull

    case Hget(k, fld) =>
      optVal(k) flatMap { _.asMap.get(fld).map( v => bytes2res(v) ) } getOrElse bulkNull

    case h: Hincrby =>

      val updatedMap = optVal(h.key).map { data =>
        val m = data.asMap
        val oldVal = m.get(h.field).map(a => bytes2int(a, ERR_INVALID_HASH_NUMBER)).getOrElse(0) + h.delta
        m.updated(h.field, int2bytes(oldVal))
      } getOrElse Map(h.field -> int2bytes(h.delta))

      map.put(h.key, Data.hash(updatedMap))

      int2res(bytes2int(updatedMap(h.field), ERR_INVALID_HASH_NUMBER))

    // set commands

    case sadd: Sadd =>
      val args = sadd.values.map(BytesWrapper).toSet
      val orig = optVal(sadd.key) map(_.asSet) getOrElse Set()
      map.put(sadd.key, Data.set(orig ++ args))
      args.diff(orig).size

    case sisMember: Sismember =>
      int2res(optVal(sisMember.key).map { data =>
        if(data.asSet.contains(BytesWrapper(sisMember.v))) 1 else 0
      } getOrElse 0)

    case Smembers(key) =>
      optVal(key) map (data =>
        MultiBulkDataResult(data.asSet.map(wrapper => bytes2res(wrapper.bytes)).toSeq)
      ) getOrElse MultiBulkDataResult(Seq())

    case eval: Eval =>
      import com.fotolog.redis.primitives.Redlock._

      // hardcoded support for Redlock implementation
      eval.script.equals(UNLOCK_SCRIPT) match {
        case true =>
          val (key, value) = eval.kv.head
          if (BytesWrapper(map.get(key).asBytes).equals(BytesWrapper(value))) {
            map.remove(key)
            1
          } else {
            0
          }

        case _ =>
          throw new RedisException(ERR_UNSUPPORTED_SCRIPT + eval.script)
      }

    case f: FlushAll =>
      map.clear()
      ok

    case p: Ping => SingleLineResult("PONG")

  }

  private[this] implicit def int2res(v: Int): Result = BulkDataResult(Some(v.toString.getBytes))

  private[this] def bytes2int(b: Array[Byte], msg: String) = try {
    new String(b).toInt
  } catch {
    case p: IllegalArgumentException =>
      throw new RedisException(msg)
  }

  private[this] def bytes2res(a: Array[Byte]) = BulkDataResult(Some(a))
  private[this] def int2bytes(i: Int): Array[Byte] = i.toString.getBytes

  private[this] val ok = SingleLineResult("OK")
  private[this] val bulkNull = BulkDataResult(None)

  private[this] def optVal(key: String) = Option(map.get(key))

  override def isOpen: Boolean = true

  override def shutdown() {}
}


case class BytesWrapper(bytes: Array[Byte]) {

  override def hashCode() = util.Arrays.hashCode(bytes)

  override def equals(obj: Any): Boolean = obj match {
    case another: BytesWrapper => util.Arrays.equals(bytes, another.bytes)
    case _ => false
  }

  override def toString = s"DateWrapper: " + new String(bytes)

}


private object ErrMessages {
  val ERR_INVALID_NUMBER = "ERR value is not an integer or out of range"
  val ERR_INVALID_HASH_NUMBER = "ERR hash value is not an integer"
  val ERR_INVALID_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
  val ERR_UNSUPPORTED_SCRIPT= "ERR Operation not support for script:"
}