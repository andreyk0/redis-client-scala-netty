package com.fotolog.redis.connections

import scala.concurrent.Future

/**
 * Common interface for Redis connections.
 *
 * @author Sergey Khruschak <sergey.khruschak@gmail.com>
 */
trait RedisConnection {

  /**
   * Sends specified command to the server and returns future of sending result.
   * @param cmd command data to send
   * @return future of received result
   */
  def send(cmd: Cmd): Future[Result]

  /**
   * @return true if connection is open.
   */
  def isOpen: Boolean

  /**
   * Closes the connection.
   */
  def shutdown()
}