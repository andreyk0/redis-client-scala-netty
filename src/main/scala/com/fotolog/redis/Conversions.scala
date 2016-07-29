package com.fotolog.redis

import java.nio.charset.Charset

import scala.annotation.implicitNotFound

@implicitNotFound(
  "No Converter found for type ${T}. Try to implement an implicit BinaryConverter for this type."
)
trait BinaryConverter[T] {
  def read(data: Array[Byte]): T
  def write(v: T) : Array[Byte]
}

object BinaryConverter extends DefaultConverters {}

trait DefaultConverters {

  val charset = Charset.forName("UTF-8")

  implicit object StringConverter extends BinaryConverter[String] {
    def read(data: Array[Byte]) = new String(data, charset)
    def write(v: String) = v.getBytes(charset)
  }

  implicit object ShortConverter extends BinaryConverter[Short] {
    def read(b: Array[Byte]) = new String(b).toShort
    def write(i: Short) = i.toString.getBytes(charset)
  }

  implicit object BooleanConverter extends BinaryConverter[Boolean] {
    def read(b: Array[Byte]) = "true" == new String(b)
    def write(v: Boolean) = v.toString.getBytes(charset)
  }

  implicit object IntConverter extends BinaryConverter[Int] {
    def read(b: Array[Byte]) = new String(b).toInt
    def write(i: Int) = i.toString.getBytes(charset)
  }

  implicit object LongConverter extends BinaryConverter[Long] {
    def read(b: Array[Byte]) = new String(b).toLong
    def write(i: Long) = i.toString.getBytes(charset)
  }

  implicit object DoubleConverter extends BinaryConverter[Double] {
    def read(b: Array[Byte]) = new String(b).toDouble
    def write(i: Double) = i.toString.getBytes(charset)
  }

  implicit object ByteArrayConverter extends BinaryConverter[Array[Byte]] {
    def read(b: Array[Byte]) = b
    def write(a: Array[Byte]) = a
  }
}
