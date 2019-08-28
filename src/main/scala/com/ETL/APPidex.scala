package com.ETL

import com.utils.RPUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object APPidex {
  def main(args: Array[String]): Unit = {
      val conf = new  SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.parquet("D:/data8_23/part-00000-5b564ce4-3ff4-4f9e-84c4-bb5642d3a1b1-c000.snappy.parquet")

    val dictionary = spark.read.textFile("D:/data8_23/app_dict.txt").rdd.map(_.split("\t",-1)).filter(_.length>=5)
      .map(arr=>(arr(4),arr(1))).collectAsMap()

     val broadcast = spark.sparkContext.broadcast(dictionary)

     df.rdd.map(row =>{
       val requestmode = row.getAs[Int]("requestmode")
       val processnode = row.getAs[Int]("processnode")
       val iseffective = row.getAs[Int]("iseffective")
       val isbilling = row.getAs[Int]("isbilling")
       val isbid = row.getAs[Int]("isbid")
       val iswin = row.getAs[Int]("iswin")
       val adorderid = row.getAs[Int]("adorderid")
       val WinPrice = row.getAs[Double]("winprice")
       val adpayment = row.getAs[Double]("adpayment")

       var appname = row.getAs[String]("appname")
       if(!StringUtils.isNoneBlank(appname)){
         appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
       }
       val reqlist =  RPUtils.request(requestmode,processnode)
       val clicklist = RPUtils.click(requestmode,iseffective)
       val adlist = RPUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)
       (appname,reqlist++clicklist++adlist)
     }).reduceByKey((list1,list2)=>{

       list1.zip(list2).map(t=>t._1+t._2)
     }).map(t=>{
       t._1 + ","+ t._2.mkString(",")
     }).foreach(println)

  }
}
