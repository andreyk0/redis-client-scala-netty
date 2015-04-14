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

  def subscribeAsync[T](channels: String*)(block: (String, T) => Unit)(implicit conv: BinaryConverter[T]): Future[Seq[Int]] = {
    r.send(Subscribe(channels, { bulkResult =>
      val Seq(_, channel, message) = bulkResult.results
      val channelName = channel.data.map(BinaryConverter.StringConverter.read).get
      val data = message.data.map(conv.read).get
      block(channelName, data)
    })).map { multiBulkDataResultToFilteredSeq(BinaryConverter.IntConverter) }
  }

  def subscribe[T](channels: String*)(block: (String, T) => Unit)(implicit conv: BinaryConverter[T]): Seq[Int] =
    await(subscribeAsync(channels:_*)(block)(conv))

  def unsubscribeAsync(channels: String*): Future[Seq[Int]] =
    r.send(Unsubscribe(channels)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.IntConverter))

  def unsubscribe(channels: String*) = await(unsubscribeAsync(channels:_*))
}
