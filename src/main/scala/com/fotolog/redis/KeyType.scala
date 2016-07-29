package com.fotolog.redis

sealed abstract class KeyType(val name: String)

object KeyType {
  def apply(s: String): KeyType = {
    s match {
      case KeyType.None.name => None
      case KeyType.String.name => String
      case KeyType.List.name => List
      case KeyType.Set.name => Set
      case KeyType.Zset.name => Zset
      case KeyType.Hash.name => Hash
    }
  }

  case object None extends KeyType("none") // key does not exist
  case object String extends KeyType("string") // binary String value, any seq of bytes
  case object List extends KeyType("list") // contains a List value
  case object Set extends KeyType("set") // contains a Set value
  case object Zset extends KeyType("zset") // contains a Sorted Set value
  case object Hash extends KeyType("hash") // contains a Hash value
}