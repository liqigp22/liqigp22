package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
    val config = new JedisPoolConfig()

    config.setMaxTotal(20)
    config.setMaxIdle(10)

    val pool = new JedisPool(config,"192.168.159.100",123456)

  def getConnection():Jedis ={
    pool.getResource

  }
}
