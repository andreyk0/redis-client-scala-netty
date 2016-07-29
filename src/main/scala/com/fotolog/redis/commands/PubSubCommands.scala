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
      val (channelBin, dataBin) = bulkResult.results match {
        case Seq(_, channelBin, messageBin) =>
          (channelBin, messageBin)
        case Seq(_, channelPatternBin, channelBin, messageBin) =>
          (channelBin, messageBin)
      }

      val channelName = channelBin.data.map(BinaryConverter.StringConverter.read).get

      try{
        block(channelName, dataBin.data.map(conv.read).get)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    })).map { multiBulkDataResultToFilteredSeq(BinaryConverter.IntConverter) }
  }

  def subscribe[T](channels: String*)(block: (String, T) => Unit)(implicit conv: BinaryConverter[T]): Seq[Int] =
    await(subscribeAsync(channels:_*)(block)(conv))

  def unsubscribeAsync(channels: String*): Future[Seq[Int]] =
    r.send(Unsubscribe(channels)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.IntConverter))

  def unsubscribe(channels: String*) = await(unsubscribeAsync(channels:_*))

  def unsubscribe = await(r.send(UnsubscribeAll()))
}
