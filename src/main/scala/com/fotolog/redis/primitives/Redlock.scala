package com.fotolog.redis.primitives

import java.security.SecureRandom
import java.util.UUID

import com.fotolog.redis.{RedisHost, RedisClient}

/**
 * Created by yaroslav on 16/03/15.
 *
 * @author Yaroslav Derman - yaroslav.derman@gmail.com
 */
class Redlock(clients: Seq[RedisClient]) {

  private val quorum = clients.length / 2 + 1

  import Redlock._
  import scala.util.control.Breaks._

  def lock(key: String, ttl: Int = DEFAULT_TTL, triesCount: Int = DEFAULT_TRIES): Lock = {
    val value = generateRandomValue

    var tries = 0

    while (tries < triesCount) {

      var successCount = 0
      val startTime = System.currentTimeMillis()

      clients.foreach { client => if (client.setNx(key, value, ttl)) successCount += 1 }

      val elapsedTime = System.currentTimeMillis() - startTime

      if (elapsedTime < ttl && successCount >= quorum) {
        return SuccessfulLock(key, value)
      } else {
        unlock(SuccessfulLock(key, value))
        tries += 1
      }
    }

    FailedLock
  }

  def unlock(lock: Lock) = lock match {
    case SuccessfulLock(key, value) =>
      clients.foreach(c => c.eval[Int](UNLOCK_SCRIPT, key -> value))
    case _ =>
  }

  protected def generateRandomValue = UUID.randomUUID().toString
}


object Redlock {
  private final val DEFAULT_TTL = 5 * 60
  private final val DEFAULT_TRIES = 3
  private[redis] final val UNLOCK_SCRIPT = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n return redis.call(\"del\", KEYS[1])\n else\n return 0\n end"

  def apply(clients: RedisClient*): Redlock = new Redlock(clients)
}

trait Lock {
  def successful = false
}

case class SuccessfulLock(key: String, value: String) extends Lock {
  override def successful = true
}

object FailedLock extends Lock


