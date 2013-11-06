package com.fotolog.redis

/**
 * Created with IntelliJ IDEA.
 * User: sergey
 * Date: 10/29/13
 * Time: 9:30 AM
 * To change this template use File | Settings | File Templates.
 */
object RedisClientTypes {
  type BinVal = Array[Byte]
  type KV = (String, BinVal)
}
