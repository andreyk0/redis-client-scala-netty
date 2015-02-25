package com.fotolog.redis.connections

import java.util.concurrent.ConcurrentHashMap

import com.fotolog.redis.KeyType

import scala.compat.Platform
import scala.concurrent.{Promise, Future}

sealed case class Data(v: AnyRef, ttl: Int = -1, keyType: KeyType = KeyType.String, stamp: Long = Platform.currentTime) {
  def asBytes = v.asInstanceOf[Array[Byte]]
  def expired = ttl != -1 && Platform.currentTime - stamp > (ttl * 1000L)
  def secondsLeft = if (ttl == -1) -1 else (ttl - (Platform.currentTime - stamp) / 1000).toInt
}

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

  override def send(cmd: Cmd): Future[Result] = Promise.successful(syncSend(cmd)).future

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
      int2res(optVal(key) map { d => map.put(key, d.copy(ttl = seconds)); 1 } getOrElse(0))

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

    case f: FlushAll =>
      map.clear()
      ok
    case p: Ping => SingleLineResult("PONG")

  }

  private[this] implicit def int2res(v: Int): Result = BulkDataResult(Some(v.toString.getBytes))

  private[this] val ok = SingleLineResult("OK")
  private[this] val bulkNull = BulkDataResult(None)

  private[this] def optVal(key: String) = Option(map.get(key))

  override def isOpen: Boolean = true

  override def shutdown() {}
}
