package com.fotolog.redis.connections

import java.nio.charset.Charset
import java.text.ParseException
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.fotolog.redis.KeyType

import scala.compat.Platform
import scala.concurrent.{Future, Promise}

sealed case class Data(v: AnyRef, ttl: Int = -1, keyType: KeyType = KeyType.String, stamp: Long = Platform.currentTime) {
  def asBytes = keyType match {
    case KeyType.String => v.asInstanceOf[Array[Byte]]
    case _ => throw new DataException("illegal type")
  }

  def asMap = keyType match {
    case KeyType.Hash => v.asInstanceOf[Map[String, Array[Byte]]]
    case _ => throw new DataException("illegal type")
  }

  def asSet = keyType match {
    case KeyType.Set => v.asInstanceOf[Set[Array[Byte]]]
    case _ => throw new DataException("illegal type")
  }

  def expired = ttl != -1 && Platform.currentTime - stamp > (ttl * 1000L)
  def secondsLeft = if (ttl == -1) -1 else (ttl - (Platform.currentTime - stamp) / 1000).toInt
}

private object Data {
  def str(d: Array[Byte], ttl: Int = -1) = Data(d, ttl, keyType = KeyType.String)
  def hash(map: Map[String, Array[Byte]], ttl: Int = -1) = Data(map, ttl, keyType = KeyType.Hash)
  def set(set: Set[Array[Byte]], ttl: Int = -1) = Data(set, ttl, keyType = KeyType.Set)
}

private case class DataException(msg: String) extends RuntimeException(msg)

object InMemoryRedisConnection {
  val fakeServers = new ConcurrentHashMap[String, ConcurrentHashMap[String, Data]]

  private[redis] val cleaner = new Runnable {
    override def run() = {
      while(true) {
        val servers = fakeServers.elements()

        while(servers.hasMoreElements) {
          val map = servers.nextElement()

          val it = map.entrySet.iterator
          while (it.hasNext) {
            if(it.next.getValue.expired) it.remove()
          }
        }

        Thread.sleep(1000)
      }
    }
  }

  private[this] val t = new Thread(cleaner)
  t.setDaemon(true)
  t.start()
}

/**
 * Fake redis connection that can be used for testing purposes.
 */
class InMemoryRedisConnection(dbName: String) extends RedisConnection {
  InMemoryRedisConnection.fakeServers.putIfAbsent(dbName, new ConcurrentHashMap[String, Data]())

  val map = InMemoryRedisConnection.fakeServers.get(dbName)

  override def send(cmd: Cmd): Future[Result] = {
    val result = try {
      syncSend(cmd)
    } catch {
      case dex: DataException =>
        ErrorResult(dex.msg)
    }

    Promise.successful(result).future
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
      optVal(hmget.key).map {
        _.asMap.filterKeys(hmget.fields.contains(_)).values.map( v => BulkDataResult(Some(v)) ) match {
          case Seq(one) => one
          case bulks: Seq[BulkDataResult] => MultiBulkDataResult(bulks)
        }
      } getOrElse bulkNull

    case Hget(k, fld) =>
      optVal(k) flatMap { _.asMap.get(fld).map( v => BulkDataResult(Some(v)) ) } getOrElse bulkNull

    case h: Hincrby =>

      val updatedMap = optVal(h.key).map { data =>
        val m = data.asMap
        val oldVal = m.get(h.field).map(bytes2int).getOrElse(0) + h.delta
        m.updated(h.field, int2bytes(oldVal))
      } getOrElse Map(h.field -> int2bytes(h.delta))

      map.put(h.key, Data.hash(updatedMap))

      int2res(bytes2int(updatedMap(h.field)))

    // set commands

    case sadd: Sadd =>

      val key = sadd.key

      optVal(key).map { data =>

        val sOld = data.asSet.map(DataWrapper)
        val sNew = sadd.values.map(DataWrapper).filterNot(sOld).toSet

        if (sNew.nonEmpty) {
          map.put(key, Data.set((sOld ++ sNew).map(_.bytes)))
        }

        int2res(sNew.size)

      } getOrElse {
        map.put(key, Data.set(Set(sadd.values:_*)))
        int2res(sadd.values.length)
      }

    case sisMember: Sismember =>

      if ( optVal(sisMember.key).map { _.asSet.map(DataWrapper).contains(DataWrapper(sisMember.v)) }.getOrElse(false) ) {
        1
      } else {
        0
      }

    case f: FlushAll =>
      map.clear()
      ok

    case p: Ping => SingleLineResult("PONG")

  }

  private[this] implicit def int2res(v: Int): Result = BulkDataResult(Some(v.toString.getBytes))

  private[this] def bytes2int(b: Array[Byte]) = try {
    new String(b).toInt
  } catch {
    case p: ParseException =>
      throw DataException("ERR hash value is not an integer")
  }

  private[this] def int2bytes(i: Int): Array[Byte] = i.toString.getBytes

  private[this] val ok = SingleLineResult("OK")
  private[this] val bulkNull = BulkDataResult(None)

  private[this] def optVal(key: String) = Option(map.get(key))

  override def isOpen: Boolean = true

  override def shutdown() {}
}

case class DataWrapper(bytes: Array[Byte]) {

  override def hashCode(): Int = {
    val prime= 31
    var result = 1

    result = prime * result + (if ((bytes == null)) 0 else bytes.hashCode())

    result
  }

  override def equals(obj: scala.Any): Boolean = obj match {

    case another: DataWrapper =>
      util.Arrays.equals(bytes, another.bytes)

    case obj: Object =>
      false

    case null =>
      false
  }


  override def toString: String = {
    val encode = new String(bytes, Charset.forName("UTF-8"))
    s"DateWrapper: " + encode
  }
}
