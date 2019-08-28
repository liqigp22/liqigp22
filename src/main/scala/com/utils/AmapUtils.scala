package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

object AmapUtils {
     def getBusinessFromAmap(long: Double,lat:Double):String ={

       val location = long +","+lat
       val urlStr  = "https://restapi.amap.com/v3/geocode/regeo?"+location+"&key=2d24d3f8f2e10bca938db3886f690fc3"
       val jsonstr =HttpUtils.get(urlStr)

     val jsonparse = JSON.parseObject(jsonstr)

      val status = jsonparse.getIntValue("status")
        if(status ==0 ) return ""

      val regeocodeJson= jsonparse.getJSONObject("regeocode")
       if(regeocodeJson == null || regeocodeJson.keySet().isEmpty)return ""

      val addressCompoentJson = regeocodeJson.getJSONObject("addressComponent")
       if(addressCompoentJson == null || addressCompoentJson.keySet().isEmpty)return ""

       val businessAreasArray = addressCompoentJson.getJSONArray("businessAresArray")
       if(businessAreasArray == null || businessAreasArray.isEmpty)return  null

         val buffer  = collection.mutable.ListBuffer[String]()

       for(item <- businessAreasArray.toArray){
         if(item.isInstanceOf[JSONObject]){
           val json = item.asInstanceOf[JSONObject]
           buffer.append(json.getString("name"))
         }
       }
       buffer.mkString(",")
     }


}
