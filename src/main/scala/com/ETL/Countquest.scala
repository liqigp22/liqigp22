package com.ETL

import com.utils.{SchemaUtils, Utils2Type}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Countquest {
  def main(args: Array[String]): Unit = {


    //确定可变的参数已经被设定
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //声明、注册基本信息，同时优化序列化方式，文件存储方式、压缩方式
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val hivecontext = new HiveContext(sc)
     //拿取并过滤数据
    val lines = sc.textFile(inputPath)
    val rowRDD = lines.map(t=>t.split(",",t.length)).filter(_.length >= 85)
      .map(arr=>{
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type.toInt(arr(73)),
          Utils2Type.toDouble(arr(74)),
          Utils2Type.toDouble(arr(75)),
          Utils2Type.toDouble(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type.toInt(arr(84))
        )
      })
     //转化df,建立中间表
    val df = sQLContext.createDataFrame(rowRDD,SchemaUtils.structtype)
        df.createOrReplaceTempView("Alldata")
    //实现内核计算逻辑 获取 原始请求数、有效请求数、广告请求数、参与竞价数、竞价成功数、展示数、点击数、DSP广告消费、DSP广告成本
    val suiyi = sQLContext.sql(" select provincename,cityname,ispid,ispname,networkmannername,networkmannerid,appname,appid,client,device,devicetype," +
      "(case when requestmode = 1 and processnode >=1 then 1 end)as orginrequests ,\n (case when requestmode =1 and processnode >=2 then 1 end )as effictiverequests,\n (case when requestmode =1 and processnode >=3 then 1 end )as Adrequests,\n (case when iseffective =1 and isbilling =1 and isbid =1  then 1 end)as biddingnumber,\n (case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 end)as winbidnum ,\n (case when requestmode =2 and iseffective =1 then 1 end)as shownumbers ,\n (case when requestmode =3 and iseffective =1 then 1 end)as clicknumbers ,\n (case when iseffective =1 and isbilling =1 and iswin = 1 then winprice/1000 end )as AdConsum,\n (case when iseffective =1 and isbilling =1 and iswin = 1 then adpayment/1000 end )as AdAdCost from Alldata ")
        suiyi.createOrReplaceTempView("areadistribution")

     //接下来只要从这个宽表中提取数据即可
     //1,通用字段 总请求数 有效请求数 广告请求数 参与竞价数 竞价成功数 竞价成功率 展示量 点击量 点击率 广告成本  广告消费
     hivecontext.sql("select orginrequests,effictiverequests,Adrequests ，biddingnumber,winbidnum,(winbidnum/biddingnumber)as winpercent ,shownumbers,clicknumbers,(clicknumbers/shownumbers)as clickpersent,AdConsum,AdAdCost from areadistribution").show()

     //sQLContext.sql("select ")






    df.write.json(outputPath)
  }
}
