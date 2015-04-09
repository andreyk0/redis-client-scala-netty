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
   * Puts connection into subscribe mode, which have only reduced commands set support and
   * forwards all received results into handler.
   *
   * @param cmd subscribe command data.
   * @param handler function used to process received messages.
   * @return future of received subscription result status
   */
  /* def subscribe(cmd: Subscribe, handler: Result => Unit): Future[Result] */

  /**
   * @return true if connection is open
   */
  def isOpen: Boolean

  /**
   * Closes connection.
   */
  def shutdown()
}