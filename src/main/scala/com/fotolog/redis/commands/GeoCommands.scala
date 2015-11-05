package com.fotolog.redis.commands

import com.fotolog.redis.{GeoUnit, BinaryConverter, RedisClient}
import com.fotolog.redis.connections._

import scala.concurrent.Future

/**
 * http://redis.io/commands#geo
 */
private[redis] trait GeoCommands extends ClientCommands {
  self: RedisClient =>

  import com.fotolog.redis.commands.ClientCommands._

  def geoAddAsync(key: String, members: (Double, Double, String)* ): Future[Int] =
    r.send(GeoAdd3(key, members.map {
      case (lat, lon, mem) => (
        BinaryConverter.DoubleConverter.write(lat),
        BinaryConverter.DoubleConverter.write(lon),
        BinaryConverter.StringConverter.write(mem)
      )
    })).map(integerResultAsInt)

  def geoAdd(key: String, members: (Double, Double, String)* ): Int = await { geoAddAsync(key, members:_*) }

  def geoAddAsync(key: String, members: (Double, String)* ): Future[Int] =
    r.send(GeoAdd2(key, members.map {
      case (lat, mem) => (
        BinaryConverter.DoubleConverter.write(lat),
        BinaryConverter.StringConverter.write(mem)
        )
    })).map(integerResultAsInt)

  def geoAdd(key: String, members: (Double, String)* ): Int = await { geoAddAsync(key, members:_*) }

  def geoDistAsync(key: String, member1: String, member2: String, unit: GeoUnit = GeoUnit.Meters): Future[Double] =
    r.send(GeoDist(key, unit.name, member1, member2)).map(doubleResultAsDouble)

  def geoDist(key: String, member1: String, member2: String, unit: GeoUnit = GeoUnit.Meters): Double =
    await { geoDistAsync(key, member1, member2, unit) }

  def geoHashAsync(key: String, members: String*): Future[Seq[String]] =
    r.send(GeoHash(key, members)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.StringConverter))

  def geoHash(key: String, members: String*): Seq[String] = await { geoHashAsync(key, members:_*) }

  def geoPosAsync(key: String, members: String*): Future[Seq[String]] =
    r.send(GeoPos(key, members)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.StringConverter))

  def geoPos(key: String, members: String*): Seq[String] = await { geoPosAsync(key, members:_*) }


}
