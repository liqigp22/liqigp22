package com.Tags

import com.utils.{BusineUtils, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object BusinessTag  extends  Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数

    //获取经纬度，过滤经纬度

    //    row.getAs[String]("long").toDouble >= 73 && row.getAs[String]("long").toDouble <= 135
    //      && row.getAs[String]("lat").toDouble >= 3 && row.getAs[String]("lat").toDouble <= 54
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")

    if (Utils2Type.toDouble(long) >= 73.0 &&
      Utils2Type.toDouble(long) <= 135.0 &&
      Utils2Type.toDouble(lat) >= 3.0 &&
      Utils2Type.toDouble(lat) <= 54.0) {
      val business = BusineUtils.getBusiness(long.toDouble, lat.toDouble)
      if (StringUtils.isNoneBlank(business)) {
        val lines = business.split(",")
        lines.foreach(f => list :+= (f, 1))
      }


    }
    list

  }
}




