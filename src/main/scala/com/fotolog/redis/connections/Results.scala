package com.fotolog.redis.connections

import scala.concurrent.Promise

private[redis] sealed abstract class Result
case class ErrorResult(err: String) extends Result
case class SingleLineResult(msg: String) extends Result
case class BulkDataResult(data: Option[Array[Byte]]) extends Result {
  override def toString =
    "BulkDataResult(%s)".format({ data match { case Some(barr) => new String(barr); case None => "" } })
}
case class MultiBulkDataResult(results: Seq[BulkDataResult]) extends Result

case class ResultFuture(cmd: Cmd) {
  val promise = Promise[Result]()
  def future = promise.future
}