package com.fotolog.redis

import com.fotolog.redis.RedisClientTypes.BinVal
import scala.annotation.implicitNotFound
import java.nio.charset.Charset

@implicitNotFound(
  "No Converter found for type ${T}. Try to implement an implicit BinaryConverter for this type."
)
trait BinaryConverter[T] {
  def read(data: BinVal): T
  def write(v: T) : BinVal
}

object BinaryConverter extends DefaultConverters {}

trait DefaultConverters {

  val charset = Charset.forName("UTF-8")

  implicit object StringConverter extends BinaryConverter[String] {
    def read(data: BinVal) = new String(data, charset)
    def write(v: String) = v.getBytes(charset)
  }

  implicit object IntConverter extends BinaryConverter[Int] {
    def read(b: BinVal) = new String(b).toInt
    def write(i: Int) = i.toString.getBytes
  }

  implicit object LongConverter extends BinaryConverter[Long] {
    def read(b: BinVal) = new String(b).toLong
    def write(i: Long) = i.toString.getBytes
  }
}
