package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row



object TagsAd  extends  Tag {

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val adType:Int = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list :+= ("LC"+v , 1)
      case v if v <=9 && v >0 => list:+=("LC0"+v ,1 )
    }
    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }
    list
  }

}