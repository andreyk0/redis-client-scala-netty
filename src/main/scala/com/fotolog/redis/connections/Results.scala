package com.fotolog.redis.connections

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
private[redis] case class ResultFuture(cmd: Cmd) {
  val promise = Promise[Result]()
  def future = promise.future
}