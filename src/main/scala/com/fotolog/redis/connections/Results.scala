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
 * Holds command and promise for response for that command.
 * Promise will be satisfied after response from server for that command.
 * @param cmd command that waits for result.
 */
private[redis] class ResultFuture(val cmd: Cmd) {
  val promise = Promise[Result]()
  def future = promise.future

  def fillWithResult(r: Result): Boolean = { promise.success(r); true }
  def fillWithFailure(err: ErrorResult) = promise.failure(new RedisException(err.err))

  def complete: Boolean = true
}


/**
 * ResultFuture that contains command which receives multiple responses.
 * Used when dealing with Subscribe/Unsubscribe commands which forces server to
 * respond with multiple BulkDataResults which here are packaged into MultiBulkDataResults.
 *
 * @param complexCmd command to handle response for
 * @param parts number of parts that command expects
 */
private[redis] case class ComplexResultFuture(complexCmd: Cmd, parts: Int) extends ResultFuture(complexCmd) {
  val responses = new ListBuffer[BulkDataResult]()

  override def fillWithResult(r: Result) = {
    responses += r.asInstanceOf[BulkDataResult]

    if(responses.length == parts) {
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