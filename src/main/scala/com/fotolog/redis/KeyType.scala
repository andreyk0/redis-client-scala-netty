package com.fotolog.redis

sealed abstract class KeyType

object KeyType {
  def apply(s: String): KeyType = {
    s match {
      case "none" => None
      case "string" => String
      case "list" => List
      case "set" => Set
      case "zset" => Zset
      case "hash" => Hash
    }
  }

  case object None extends KeyType // key does not exist
  case object String extends KeyType // binary String value, any seq of bytes
  case object List extends KeyType // contains a List value
  case object Set extends KeyType // contains a Set value
  case object Zset extends KeyType // contains a Sorted Set value
  case object Hash extends KeyType // contains a Hash value
}