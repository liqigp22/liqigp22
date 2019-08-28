package Text8_24

import com.alibaba.fastjson.{JSON, JSONObject}

object Type {
  /**
    *   TYpe的JSon筛选，获取Type文件
    *
    * @param args
    * @return
    */


  def getTypeFromap(args: Any*): String = {

    val jsonstr = args(0).asInstanceOf[String]
    val jsonparse = JSON.parseObject(jsonstr)
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""

    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if (regeocodeJson == null || regeocodeJson.keySet().isEmpty)return ""

    val poisJson = regeocodeJson.getJSONArray("pois")
    if (poisJson == null || poisJson.isEmpty)return null

    val buffer = collection.mutable.ListBuffer[String]()

    for(item <- poisJson.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("type"))
      }
    }
    buffer.mkString(",")
  }



}
