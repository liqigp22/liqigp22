package com.GraphX


import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object graph_text {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
      (1L, ("詹姆斯", 35)),
      (2L, ("霍华德", 34)),
      (6L, ("杜兰特", 31)),
      (9L, ("库里", 30)),
      (133L, ("哈登", 30)),
      (138L, ("席尔瓦", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (22L, ("J罗", 28)),
      (21L, ("高斯林", 60)),
      (5L, ("奥德斯基", 55)),
      (158L, ("马云", 55))

    ))

    val edge: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 133L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

     val graph = Graph(vertexRDD,edge)

    val vertics = graph.connectedComponents().vertices
    vertics.join(vertexRDD).map{
      case (userId,(conId,(name,age)))=>{
        (conId,List(name,age))
      }
    }.reduceByKey(_++_).foreach(println)

  }
}
