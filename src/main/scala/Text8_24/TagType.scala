package Text8_24

import org.apache.commons.lang3.StringUtils



object TagType {
  /**
    * 对Type进行打标签
    *
    */


  def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val typestr = args(0).asInstanceOf[String]

    // 根据分号切割获取数据
    val kwds = typestr.split(";").toString

    // 进行判断并打标签
    if (StringUtils.isNoneBlank(kwds)) {
      list :+= ("Type" + kwds, 1)
    }

    list
  }
}
