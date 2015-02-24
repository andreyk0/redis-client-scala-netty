package com.fotolog.redis.connections

import java.util.concurrent.ConcurrentHashMap

import com.fotolog.redis.KeyType

import scala.compat.Platform
import scala.concurrent.{Promise, Future}

sealed case class Data(v: AnyRef, ttl: Int = -1, keyType: KeyType = KeyType.String, stamp: Long = Platform.currentTime) {
  def asBytes = v.asInstanceOf[Array[Byte]]
  def expired = ttl != -1 && Platform.currentTime - stamp > (ttl * 1000)
}

object InMemoryRedisConnection {
  val fakeServers = new ConcurrentHashMap[String, ConcurrentHashMap[String, Data]]
}

/**
 * Fake redis connection that can be used for testing purposes.
 */
class InMemoryRedisConnection(dbName: String) extends RedisConnection {
  InMemoryRedisConnection.fakeServers.putIfAbsent(dbName, new ConcurrentHashMap[String, Data]())

  val map = InMemoryRedisConnection.fakeServers.get(dbName)

  override def send(cmd: Cmd): Future[Result] = Promise.successful(syncSend(cmd)).future

  private[this] def syncSend(cmd: Cmd): Result = cmd match {
    case set: SetCmd if set.nx =>
        Option( map.putIfAbsent(set.key, Data(set.v, set.expTime)) ) map(_ => bulkNull) getOrElse(ok)

    case set: SetCmd if set.xx =>
      Option( map.replace(set.key, Data(set.v, set.expTime)) ) map (_ => ok) getOrElse(bulkNull)

    case set: SetCmd =>
      map.put(set.key, Data(set.v, set.expTime))
      ok

    case Get(key) =>
      BulkDataResult(
        Option(map.get(key)) filterNot(_.expired) map (_.asBytes)
      )

    case Exists(key) =>
      if(map.containsKey(key)) 1 else 0

    case Type(key) =>
      SingleLineResult(
        Option (map.get(key) ) map ( _.keyType.name ) getOrElse KeyType.None.name
      )

    case d: Del =>
      d.keys.count(k => Option(map.remove(k)).isDefined)

    case f: FlushAll =>
      map.clear()
      ok
    case p: Ping => SingleLineResult("PONG")

  }

  private[this] implicit def int2res(v: Int): Result = BulkDataResult(Some(v.toString.getBytes))

  private[this] val ok = SingleLineResult("OK")
  private[this] val bulkNull = BulkDataResult(None)

  override def isOpen: Boolean = true

  override def shutdown() {}
}
