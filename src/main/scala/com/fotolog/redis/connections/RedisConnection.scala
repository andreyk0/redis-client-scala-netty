package com.fotolog.redis.connections

import scala.concurrent.Future

trait RedisConnection {
  def send(cmd: Cmd): Future[Result]
  def isOpen: Boolean
  def shutdown()
}
