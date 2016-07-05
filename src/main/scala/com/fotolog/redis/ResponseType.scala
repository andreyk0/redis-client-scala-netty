package com.fotolog.redis

/**
 * Created with IntelliJ IDEA.
 * User: sergey
 * Date: 10/29/13
 * Time: 9:33 AM
 * To change this template use File | Settings | File Templates.
 */
private[redis] object ResponseType {
  def apply(b: Byte): ResponseType = {
    b match {
      case Error.b => Error
      case SingleLine.b => SingleLine
      case BulkData.b => BulkData
      case MultiBulkData.b => MultiBulkData
      case Integer.b => Integer
    }
  }
}
private[redis] sealed abstract class ResponseType(val b: Byte)
private[redis] case object Error extends ResponseType('-')
private[redis] case object SingleLine extends ResponseType('+')
private[redis] case object BulkData extends ResponseType('$')
private[redis] case object MultiBulkData extends ResponseType('*')
private[redis] case object Integer extends ResponseType(':')

private[redis] case object Unknown extends ResponseType('?')
private[redis] case class BinaryData(len: Int) extends ResponseType('B')
private[redis] case object NullData extends ResponseType('N')
