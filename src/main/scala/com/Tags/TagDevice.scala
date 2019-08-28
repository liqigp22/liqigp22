package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagDevice  extends  Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val clientType = row.getAs[Int]("client")
    clientType match {
      case v if v == 1 => list :+= ("Andriod"+"_"+"D0001000" + v, 1)
      case v if v == 2 => list :+= ("IOS"+"_"+"D0001000" + v, 1)
      case v if v == 3 => list :+= ("WinPhone"+"_"+"D0001000" + v, 1)
      case _ => list :+=("其它"+"D00010004",1)
    }

    val networkmanneridType: Int = row.getAs[Int]("networkmannerid")
    networkmanneridType match {
      case v if v == 1 => list :+= ("WIFI"+"_"+"D0002000" + v, 1)
      case v if v == 2 => list :+= ("４G"+"_"+"D0002000" + v, 1)
      case v if v == 3 => list :+= ("３G"+"_"+"D0002000" + v, 1)
      case v if v == 4 => list :+= ("2G"+"_"+"D0002000" + v, 1)
      case _ => list :+=("_"+"D00020005",1)
    }

    val ispidType: Int = row.getAs[Int]("ispid")
    ispidType match {
      case v if v == 1 => list :+= ("移动"+"_"+"D000300" + v, 1)
      case v if v == 2 => list :+= ("联通"+"_"+"D000300" + v, 1)
      case v if v == 3 => list :+= ("电信"+"_"+"D000300" + v, 1)
      case _ => list :+=("_"+"D00030004",1)
    }


    list
  }
}
