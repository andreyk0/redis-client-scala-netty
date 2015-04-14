package com.fotolog.redis.connections

import com.fotolog.redis.RedisException

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise

private[redis] sealed abstract class Result
case class ErrorResult(err: String) extends Result
case class SingleLineResult(msg: String) extends Result
case class BulkDataResult(data: Option[Array[Byte]]) extends Result {
  override def toString =
    "BulkDataResult(%s)".format(data.map(d => new String(d)).getOrElse(""))
}
case class MultiBulkDataResult(results: Seq[BulkDataResult]) extends Result

/**
 *
 * @param cmd
 */
private[redis] class ResultFuture(val cmd: Cmd) {
  val promise = Promise[Result]()
  def future = promise.future

  def fillWithResult(r: Result): Boolean = { promise.success(r); true }
  def fillWithFailure(err: ErrorResult) = promise.failure(new RedisException(err.err))

  def complete: Boolean = true
}

private[redis] case class ComplexResultFuture(complexCmd: Cmd, parts: Int) extends ResultFuture(complexCmd) {
  val responses = new ListBuffer[BulkDataResult]()

  println("Created complex result with parts: " + parts)

  override def fillWithResult(r: Result) = {
    responses += r.asInstanceOf[BulkDataResult]

    if(responses.length == parts) {
      println("Filled complex response")
      promise.success(MultiBulkDataResult(responses.toSeq))
      true
    } else {
      false
    }
  }

  override def complete = responses.length == parts
}

object ResultFuture {
  def apply(cmd: Cmd) = {
    cmd match {
      case scrb: Subscribe =>
        new ComplexResultFuture(scrb, scrb.channels.length)
      case unscrb: Unsubscribe =>
        new ComplexResultFuture(unscrb, unscrb.channels.length)
      case any: Cmd =>
        new ResultFuture(cmd)
    }
  }

}