package com.fotolog.redis.commands

import com.fotolog.redis._
import com.fotolog.redis.connections._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * http://redis.io/commands#pubsub
 */
private[redis] trait PubSubCommands extends ClientCommands {
  import com.fotolog.redis.commands.ClientCommands._

  def subscribe[T](channels: String*)(block: String => Unit)(implicit conv: BinaryConverter[T]): Unit = {
    r.send(Subscribe(channels)).map(multiBulkDataResultToSet(conv))
  }

}
