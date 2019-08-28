package com.Tags

import com.utils.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}




object TagsContext {
  /**
   制作人 李琪
  制作日期 8_23
  */
  def main(args: Array[String]): Unit = {
    //先确定有对应数量的数据传入
     if(args.length != 3){
          println( "目录不匹配，退出程序"  )
        sys.exit()
     }
    // 传入数据
    val Array(inputpath1,inputpath2,inputpath3 ) = args

      //写入sparkconf对象，注册SparkSession上下文
    val  conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.parquet(inputpath1)

    //广播变量，别问问就是第一步获取需要广播的数据信息，collectasMap进行收集，第二步进行广播
    val bromap: collection.Map[String, String] = spark.read.textFile(inputpath2).rdd.map(_.split("\t",-1)).filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    val broadcast =spark.sparkContext.broadcast(bromap)

    //redis 读取 先通过jedis连接 ，然后第二步jedis需要set获取数据，之前拿到的bromap本身就是两个需要的string  id 和 name ,只要拆分开输出即可
//      val jedis = new Jedis("192.168.159.100",6379)
//      bromap.foreach(x=>jedis.set(x._1,x._2))
     val dada = spark.read.textFile(inputpath3).rdd.map((_,0)).collectAsMap()
     val dadab = spark.sparkContext.broadcast(dada)

    //把所有的符合条件的（满足15个唯一id的）信息进行筛选，调用各个方法进行打标签
    val UseTag = df.filter(TagUtils.OneUsrId)
      .rdd.map(row => {
      val userId = TagUtils.getOnUserId(row)
      val adtag = TagsAd.makeTags(row)
      val appList = TagAPP.makeTags(row,broadcast)
      val  provinceandcityname    = TagArea.makeTags(row)
      val  clintname = TagDevice.makeTags(row)
      val  adplatformproviderid = Tagadplat.makeTags(row)
      val   stopkeyword  = TagStopWord.makeTags(row,dadab)
      val   businesslist  = BusinessTag.makeTags(row)
      (userId, adtag++appList++provinceandcityname++clintname++adplatformproviderid++stopkeyword++businesslist)
    }).reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }
    ).foreach(println)




  }
}
