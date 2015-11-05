package com.fotolog.redis

sealed abstract class GeoUnit(val name: String)

object GeoUnit {
  case object Meters extends GeoUnit("m")
  case object Kilometers extends GeoUnit("km")
  case object Miles extends GeoUnit("mi")
  case object Feet extends GeoUnit("ft")
}
