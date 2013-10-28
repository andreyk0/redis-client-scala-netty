package com.fotolog.redis

import com.fotolog.redis.RedisClientTypes.BinVal
import scala.annotation.implicitNotFound

@implicitNotFound(
  "No Converter found for type ${T}. Try to implement an implicit BinaryConverter for this type."
)
trait BinaryConverter[T] {
  def read(data: BinVal): T
  def write(v: T) : BinVal
}

object BinaryConverter extends DefaultConverters {}

trait DefaultConverters {

  implicit object StringConverter extends BinaryConverter[String] {
    def read(data: BinVal) = new String(data)
    def write(v: String) = v.getBytes
  }

  implicit object IntConverter extends BinaryConverter[Int] {
    def read(b: BinVal) = {
      (b(0).asInstanceOf[Int] & 0xFF)       |
        (b(1).asInstanceOf[Int] & 0xFF) << 8  |
        (b(2).asInstanceOf[Int] & 0xFF) << 16 |
        (b(3).asInstanceOf[Int] & 0xFF) << 24
    }

    def write(i: Int) = Array[Byte](
      ((i     )&0xFF).asInstanceOf[Byte],
      ((i>>  8)&0xFF).asInstanceOf[Byte],
      ((i>> 16)&0xFF).asInstanceOf[Byte],
      ((i>> 24)&0xFF).asInstanceOf[Byte]
    )
  }

  implicit object LongConverter extends BinaryConverter[Long] {
    def read(b: BinVal) = {
      (b(0).asInstanceOf[Long] & 0xFF)       |
        (b(1).asInstanceOf[Long] & 0xFF) <<  8 |
        (b(2).asInstanceOf[Long] & 0xFF) << 16 |
        (b(3).asInstanceOf[Long] & 0xFF) << 24 |
        (b(4).asInstanceOf[Long] & 0xFF) << 32 |
        (b(5).asInstanceOf[Long] & 0xFF) << 40 |
        (b(6).asInstanceOf[Long] & 0xFF) << 48 |
        (b(7).asInstanceOf[Long] & 0xFF) << 56
    }

    def write(i: Long) = Array[Byte](
      ((i     )&0xFF).asInstanceOf[Byte],
      ((i>>  8)&0xFF).asInstanceOf[Byte],
      ((i>> 16)&0xFF).asInstanceOf[Byte],
      ((i>> 24)&0xFF).asInstanceOf[Byte],
      ((i>> 32)&0xFF).asInstanceOf[Byte],
      ((i>> 40)&0xFF).asInstanceOf[Byte],
      ((i>> 48)&0xFF).asInstanceOf[Byte],
      ((i>> 56)&0xFF).asInstanceOf[Byte]
    )
  }
}
