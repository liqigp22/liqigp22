package com.ETL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object qunima {
  def main(args: Array[String]): Unit = {
//    if(args.length != 2){
//      println("目录参数不正确，退出程序")
//      sys.exit()
//    }
//    val Array(inputpath,outputpath) = args
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()

      spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")


    val df: DataFrame = spark.read.parquet("D:/data8_23/part-00000-5b564ce4-3ff4-4f9e-84c4-bb5642d3a1b1-c000.snappy.parquet")
            df.createOrReplaceTempView("Alldata")
    val suiyi = spark.sql(" select provincename,cityname,ispid,ispname,networkmannername,networkmannerid,appname,appid,client,device,devicetype," +
      "(case when requestmode = 1 and processnode >=1 then 1 else 0 end)as orginrequests ," +
      " (case when requestmode =1 and processnode >=2 then 1 else 0 end )as effictiverequests," +
      " (case when requestmode =1 and processnode >=3 then 1 else 0 end )as Adrequests," +
      " (case when iseffective =1 and isbilling =1 and isbid =1  then 1 else 0 end)as biddingnumber," +
      " (case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end)as winbidnum ," +
      " (case when requestmode =2 and iseffective =1 then 1 else 0 end)as shownumbers ," +
      "(case when requestmode =3 and iseffective =1 then 1 else 0 end)as clicknumbers ," +
      " (case when iseffective =1 and isbilling =1 and iswin = 1 then winprice/1000 else 0 end )as AdConsum," +
      " (case when iseffective =1 and isbilling =1 and iswin = 1 then adpayment/1000 else 0 end )as AdAdCost from Alldata ")
    suiyi.createOrReplaceTempView("areadistribution")

    //接下来只要从这个宽表中提取数据即可
    //1,通用字段 总请求数 有效请求数 广告请求数 参与竞价数 竞价成功数 竞价成功率 展示量 点击量 点击率 广告成本  广告消费
   // spark.sql("select biddingnumber,winbidnum,(winbidnum/biddingnumber)as winpercent ,shownumbers,clicknumbers,(clicknumbers/shownumbers)as clickpersent,AdConsum,AdAdCost from areadistribution")

    spark.sql("select provincename, cityname, sum(orginrequests),sum(effictiverequests),sum(Adrequests)" +
      "from areadistribution group by provincename, cityname ").show()

// Vritual Area table
    val areaflow = spark.sql("select provincename, cityname, " +
      "sum(orginrequests)as orginrequests," +
      "sum(effictiverequests)as effictiverequests," +
      "sum(Adrequests) as Adrequests," +
      "sum( biddingnumber) as biddingnumber," +
      "sum(winbidnum) as winbidnum," +
      "(winbidnum/biddingnumber)as winpercent ," +
      "sum(shownumbers) as shownumbers," +
      "sum(clicknumbers)as clicknumbers," +
      "(clicknumbers/shownumbers)as clickpersent," +
      "AdAdCost ," +
      "AdConsum " +
      "from areadistribution group by provincename, cityname ,winbidnum,biddingnumber,shownumbers,clicknumbers,AdAdCost,AdConsum").show()





    // 运营商
    spark.sql("select (case when ispid=1 then '电信'" +
      " when ispid= 2  then '联通'" +
      " when ispid= 3 then '移动'" +
      " else '其他' end )ispname," +
      "sum(orginrequests) as orginrequests," +
      "sum(effictiverequests) as effictiverequests," +
      "sum(Adrequests) as Adrequests," +
      "sum( biddingnumber) as biddingnumber," +
      "sum(winbidnum) as winbidnum," +
      "(winbidnum/biddingnumber)as winpercent ," +
      "sum(shownumbers) as shownumbers," +
      "sum(clicknumbers)as clicknumbers," +
      "(clicknumbers/shownumbers)as clickpersent," +
      "AdAdCost ," +
      "AdConsum " +
      "from areadistribution group by ispid, ispname ,winbidnum,biddingnumber,shownumbers,clicknumbers,AdAdCost,AdConsum").show()

    //运营商 4G
      spark.sql(" select (case when networkmannername='2G' then '2G'" +
        " when networkmannername='3G' then '3G'" +
        " when networkmannername='4G' then '4G'" +
        " when networkmannername='WiFi' then 'WiFi'" +
        " else '其他' end )as networkstype," +
        "networkmannerid," +
       "sum(orginrequests) as orginrequests," +
        "sum(effictiverequests) as effictiverequests," +
      "sum(Adrequests) as Adrequests," +
      "sum( biddingnumber) as biddingnumber," +
      "sum(winbidnum) as winbidnum," +
      "(winbidnum/biddingnumber)as winpercent ," +
      "sum(shownumbers) as shownumbers," +
      "sum(clicknumbers)as clicknumbers," +
      "(clicknumbers/shownumbers)as clickpersent," +
      "AdAdCost ," +
      "AdConsum " +
      "from areadistribution group by networkstype,networkmannerid ,winbidnum,biddingnumber,shownumbers,clicknumbers,AdAdCost,AdConsum").show()

   //设备类型，
     spark.sql("select (case when devicetype=1 then '手机'" +
       "when devicetype=2 then '平板'" +
    "else '其他' end )as devicetypename," +
    "devicetype," +
    "sum(orginrequests) as orginrequests," +
      "sum(effictiverequests) as effictiverequests," +
      "sum(Adrequests) as Adrequests," +
      "sum( biddingnumber) as biddingnumber," +
      "sum(winbidnum) as winbidnum," +
      "(winbidnum/biddingnumber)as winpercent ," +
      "sum(shownumbers) as shownumbers," +
      "sum(clicknumbers)as clicknumbers," +
      "(clicknumbers/shownumbers)as clickpersent," +
      "AdAdCost ," +
      "AdConsum " +
      "from areadistribution group by devicetype,devicetypename ,winbidnum,biddingnumber,shownumbers,clicknumbers,AdAdCost,AdConsum").show()


    //操作系统
    spark.sql("select (case when client=1 then 'Andriod'" +
      " when client=2 then 'IOS'" +
      " when client=3 then 'wp'" +
      " else '其他' end )as opsystem," +
      " client,"+
      "sum(orginrequests) as orginrequests," +
      "sum(effictiverequests) as effictiverequests," +
      "sum(Adrequests) as Adrequests," +
      "sum( biddingnumber) as biddingnumber," +
      "sum(winbidnum) as winbidnum," +
      "(winbidnum/biddingnumber)as winpercent ," +
      "sum(shownumbers) as shownumbers," +
      "sum(clicknumbers)as clicknumbers," +
      "(clicknumbers/shownumbers)as clickpersent," +
      "AdAdCost ," +
      "AdConsum " +
      "from areadistribution group by client,opsystem ,winbidnum,biddingnumber,shownumbers,clicknumbers,AdAdCost,AdConsum").show()


      //val tt = sc.textFile("C:\\Program Files\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\app_dict.txt").flatMap(_.split("//t"))



    //媒体
     spark.sql("select appname," +
       "appid," +
       "sum(orginrequests) as orginrequests," +
       "sum(effictiverequests) as effictiverequests," +
       "sum(Adrequests) as Adrequests," +
       "sum( biddingnumber) as biddingnumber," +
       "sum(winbidnum) as winbidnum," +
       "(winbidnum/biddingnumber)as winpercent ," +
       "sum(shownumbers) as shownumbers," +
       "sum(clicknumbers)as clicknumbers," +
       "(clicknumbers/shownumbers)as clickpersent," +
       "AdAdCost ," +
       "AdConsum " +
       "from areadistribution group by appname,appid ,winbidnum,biddingnumber,shownumbers,clicknumbers,AdAdCost,AdConsum").show()




     spark.stop()


  }
}
