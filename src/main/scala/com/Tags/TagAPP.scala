package com.Tags


import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


object TagAPP extends  Tag{
  override def makeTags(args: Any*): List[(String, Int)] ={
    var list  = List[(String,Int)]()

    /**redis需要的步骤是知道key 获取 value ，所以声明一个id ,进去直接获取name即可，
      * 注意需要在这里再连接一下数据库，还有一个问题就是如果redis在虚拟机出现了保护模式，请使用错误文档中的方法
      */
    //    val jedis = new Jedis("192.168.159.100",6379)
    //       val appname =jedis.get(appid)


  val row = args(0).asInstanceOf[Row]
  val bromap = args(1).asInstanceOf[Broadcast[Map[String,String]]]

     val appname = row.getAs[String]("appname")
    val appid  = row.getAs[String]("appid")

    if(StringUtils.isNoneBlank(appname)){
      list:+=("APP"+appname,1)
    }else if (StringUtils.isNoneBlank(appid)) {
      list :+= ("APP" + bromap.value.getOrElse(appid, appid), 1)
    }
    list
  }
}
