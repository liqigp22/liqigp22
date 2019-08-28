package com.utils

import ch.hsr.geohash.GeoHash

object BusineUtils {
   def getBusiness(long: Double,lat:Double):String={
     val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
     val business = redis_queryBusiness(geohash)
      if (business==null || business.length == 0){
      val   business = AmapUtils.getBusinessFromAmap(long.toDouble,lat.toDouble)
        redis_insertBusiness(geohash,business)
      }
     business
   }

  def redis_queryBusiness(geohash:String):String={
    val jedis = DBConnectionPool.getConn()
    val business = jedis.getClientInfo(geohash)
    jedis.close()
    business

  }

  def redis_insertBusiness(geoHash:String,business:String):Unit ={
   val jedis =JedisConnectionPool.getConnection()
    jedis.set(geoHash,business)
    jedis.close()
  }

}
