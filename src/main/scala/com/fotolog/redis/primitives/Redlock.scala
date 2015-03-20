package com.fotolog.redis.primitives

import java.util.UUID
import com.fotolog.redis.{RedisCluster, RedisClient}

/**
 * Distributed lock on multiple (or one) redis instances.
 * Usage example:
 *
 * {{{
 *   val redlock = Redlock("192.168.2.11" -> 6379, "192.168.2.12" -> 6379, "192.168.2.13" -> 6379)
 *   val lock = redlock.lock("resource-name")
 *
 *   if(lock.successful) {
 *     /* Some code that has to be executed only on one instance/thread. */
 *   }
 *
 *   redlock.unlock(lock)
 * }}}
 *
 *  Or using more convenient method:
 *
 *  {{{
 *    val redlock = Redlock("192.168.2.15")
 *    redlock.withLock("resource-name", unlock = true) {
 *      /* Some code that has to be executed only on one instance/thread. */
 *    }
 *  }}}
 *
 * Note that unlock is not necessary as lock will be auto unlocked after it's time to live will pass.
 * Supported by in-memory client.
 *
 * @author Yaroslav Derman - yaroslav.derman@gmail.com
 */
class Redlock(clients: Seq[RedisClient]) {

  private val quorum = clients.length / 2 + 1

  import Redlock._

  /**
   * Tries to retrieve distributed lock for specified resource name.
   * Note that entry for specified resource name will be created in Redis instances.
   * Returned lock instance can be used to unlock lock manually or to retrieve lock obtaining status.
   * Manual unlock in most cases is not necessary as lock will be cleared automatically.
   *
   * Usage example:
   *
   * {{{
   *   val lock = redlock.lock("resource-name")
   *
   *   if(lock.successful) {
   *     /* Some code */
   *   }
   *
   *   redlock.unlock(lock)
   * }}}
   *
   * @param name Key to retrieve lock for
   * @param ttl lock will be destroyed automatically after specified seconds passed.
   * @param triesCount number of tries to retrieve lock.
   *
   * @return lock instance
   */
  def lock(name: String, ttl: Int = DEFAULT_TTL, triesCount: Int = DEFAULT_TRIES): Lock = {
    val value = generateRandomValue

    var tries = 0

    while (tries < triesCount) {

      var successCount = 0
      val startTime = System.currentTimeMillis()

      clients.foreach { client => if (client.setNx(name, value, ttl)) successCount += 1 }

      val elapsedTime = System.currentTimeMillis() - startTime

      if (elapsedTime < ttl && successCount >= quorum) {
        return SuccessfulLock(name, value)
      } else {
        unlock(SuccessfulLock(name, value))
        tries += 1
      }
    }

    FailedLock
  }

  /**
   * Unlocks specified locked resource on all servers.
   *
   * @param lock instance of lock that was returned by [[Redlock.lock()]] method
   */
  def unlock(lock: Lock) = lock match {
    case SuccessfulLock(key, value) =>
      clients.foreach(c => c.eval[Int](UNLOCK_SCRIPT, key -> value))
    case _ =>
  }

  /**
   * Executed some code only in case of successful lock obtaining or skips it otherwise.
   * Returns execution status: true if code was executed, false otherwise.
   *
   * @param name resource name to lock.
   * @param ttl lock will be destroyed automatically after specified seconds passed.
   * @param triesCount number of tries to retrieve lock.
   * @param unlock indicated that lock has to be unlocked after code is executed.
   * @param block block of code to execute
   *
   * @return true if lock obtained and code was executed.
   */
  def withLock(name: String, ttl: Int = DEFAULT_TTL, triesCount: Int = DEFAULT_TRIES, unlock: Boolean = false)(block: => Unit): Boolean = {
    lock(name, ttl, triesCount) match {
      case success: SuccessfulLock =>
        try { block } finally { if(unlock) this.unlock(success) }
        true
      case _ =>
        false
    }
  }

  protected def generateRandomValue = UUID.randomUUID().toString
}


object Redlock {
  private final val DEFAULT_TTL = 5 * 60
  private final val DEFAULT_TRIES = 3
  private[redis] final val UNLOCK_SCRIPT = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n return redis.call(\"del\", KEYS[1])\n else\n return 0\n end"

  def apply(client: RedisClient): Redlock = new Redlock(Seq(client))
  def apply(hosts: (String, Int)*): Redlock = new Redlock(hosts.map( h => RedisClient(h._1, h._2)))
  def apply(clients: Array[RedisClient]): Redlock = new Redlock(clients)
  def apply[T](host: String, port: Int = 6379): Redlock = new Redlock(Seq(RedisClient(host, port)))
}

trait Lock {
  def successful = false
}

case class SuccessfulLock(key: String, value: String) extends Lock {
  override def successful = true
}

object FailedLock extends Lock


