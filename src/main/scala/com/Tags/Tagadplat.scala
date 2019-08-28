package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object Tagadplat extends  Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //我现在拿到的args（0） 是输入的第一个元素，也就是整个数据，然后把它强转成Row类型，以便后续操作
    val row = args(0).asInstanceOf[Row]
    //然后需要对各个属性进行操作,第一步就是需要拿到具体的字段
    val adplatformprovideridType  = row.getAs[Int]("adplatformproviderid")

      list:+=("CN"+adplatformprovideridType,1)

    list
  }
}
