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

  def publishAsync[T](channel: String, message: T)(implicit conv: BinaryConverter[T]) =
    r.send(Publish(channel, conv.write(message))).map(integerResultAsInt)

  def publish[T](channel: String, message: T)(implicit conv: BinaryConverter[T]) = await(publishAsync(channel, message)(conv))

  def subscribe[T](channels: String*)(block: (String, T) => Unit)(implicit conv: BinaryConverter[T]): Unit = {
    r.send(Subscribe(channels, { bulkResult =>
      val Seq(_, channel, message) = bulkResult.results
      val channelName = channel.data.map(BinaryConverter.StringConverter.read).get
      val data = message.data.map(conv.read).get
      block(channelName, data)
    })).map(subscribeResult =>
        println("Outer Subscribe res: " + subscribeResult)
      )
  }

  def unsubscribe(channels: String*) = r.send(Unsubscribe(channels))
}
