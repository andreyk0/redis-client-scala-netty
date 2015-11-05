package com.fotolog.redis.commands

import com.fotolog.redis._
import com.fotolog.redis.connections._

import scala.collection.Set
import scala.concurrent.Future

private[redis] trait ClientCommands {
  protected val r: RedisConnection
  def await[T](f: Future[T]): T
}

private[commands] object ClientCommands {

  val integerResultAsBoolean: PartialFunction[Result, Boolean] = {
    case BulkDataResult(Some(v)) => BinaryConverter.IntConverter.read(v) > 0
    case BulkDataResult(None) => throw new RuntimeException("Unknown integer type")
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  val okResultAsBoolean: PartialFunction[Result, Boolean] = {
    case SingleLineResult("OK") => true
    case BulkDataResult(None) => false
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
    // for cases where any other val should produce an error
  }

  val integerResultAsInt: PartialFunction[Result, Int] = {
    case BulkDataResult(Some(v)) => BinaryConverter.IntConverter.read(v)
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  val doubleResultAsDouble: PartialFunction[Result, Double] = {
    case BulkDataResult(Some(v)) => BinaryConverter.DoubleConverter.read(v)
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  def bulkDataResultToOpt[T](convert: BinaryConverter[T]): PartialFunction[Result, Option[T]] = {
    case BulkDataResult(data) => data.map(convert.read)
  }

  def multiBulkDataResultToFilteredSeq[T](conv: BinaryConverter[T]): PartialFunction[Result, Seq[T]] = {
    case MultiBulkDataResult(results) => filterEmptyAndMap(results, conv)
  }

  def multiBulkDataResultToSet[T](conv: BinaryConverter[T]): PartialFunction[Result, Set[T]] = {
    case MultiBulkDataResult(results) => filterEmptyAndMap(results, conv).toSet
  }

  def multiBulkDataResultToMap[T](keys: Seq[String], conv: BinaryConverter[T]): PartialFunction[Result, Map[String,T]] = {
    case BulkDataResult(data) => data match {
      case None => Map()
      case Some(d) => Map(keys.head -> conv.read(d))
    }

    case MultiBulkDataResult(results) =>
      keys.zip(results).filter {
        case (k, BulkDataResult(Some(_))) => true
        case (k, BulkDataResult(None)) => false
      }.map { kv => kv._1 -> conv.read(kv._2.data.get) }.toMap
  }

  def bulkResultToSet[T](conv: BinaryConverter[T]): PartialFunction[Result, Set[T]] = {
    case MultiBulkDataResult(results) => filterEmptyAndMap(results, conv).toSet
    case BulkDataResult(Some(v)) => Set(conv.read(v))
  }


  private[this] def filterEmptyAndMap[T](r: Seq[BulkDataResult], conv: BinaryConverter[T]) = r.filter {
    case BulkDataResult(Some(_)) => true
    case BulkDataResult(None) => false
  }.map { r => conv.read(r.data.get) }
}

