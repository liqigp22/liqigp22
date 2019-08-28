package com.Tags
import com.utils.AmapUtils
import org.apache.spark.{SparkConf, SparkContext}
object test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val list= List("116.310003,39.991957")
//    val rdd = sc.makeRDD(list)
//    val bs = rdd.map(t=> {
//      val arr = t.split(",")
//      AmapUtils.getBusinessFromAmap(arr(0).toDouble,arr(1).toDouble)
//    })
val str: String = AmapUtils.getBusinessFromAmap(116.310003,39.991957)
    str.foreach(println)



  }
}