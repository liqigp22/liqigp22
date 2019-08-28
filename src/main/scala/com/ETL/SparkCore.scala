package com.ETL

import com.utils.RPUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkCore {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sqlContext.sparkSession.sqlContext.sparkSession.sqlContext.sparkSession.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    val df = spark.read.parquet("D:/data8_23/part-00000-5b564ce4-3ff4-4f9e-84c4-bb5642d3a1b1-c000.snappy.parquet")
    df.rdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")

      val reqlist = RPUtils.request(requestmode, processnode)
      val adlist = RPUtils.click(requestmode, iseffective)
      val clicklist = RPUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winPrice, adorderid)

      ((pro, city), reqlist ++ clicklist ++ adlist)
    }).reduceByKey(
      (list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      }
    ).map(t => {
      t._1 + "," + t._2.mkString(",")
    }).foreach(println)









  }

}