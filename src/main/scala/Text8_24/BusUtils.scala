package Text8_24

import com.alibaba.fastjson.{JSON, JSONObject}

object BusUtils {

  /**
    *  按照pois，分类businessarea
    *  调用方法传入数据
    * @return  输出切分好的json
    */



    def getBusinessFromap(args: Any*): String= {

       val jsonstr = args(0).asInstanceOf[String]
      val jsonparse = JSON.parseObject(jsonstr)
       val status = jsonparse.getIntValue("status")
       if(status == 0) return null

      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty)return null

      val poisJson = regeocodeJson.getJSONArray("pois")
      if (poisJson == null || poisJson.isEmpty)return null


      val buffer = collection.mutable.ListBuffer[String]()
      // 循环输出
      for(item <- poisJson.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      buffer.mkString(",")
    }



}
