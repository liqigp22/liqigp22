package com.VIP8_27

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object littlefriend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val lits1 = List("1	小红 20", "3 小明 33", "5	小七	20", "7	小王 60", "9 小李	20", "11	小美	30", "13 小花")
    val point = spark.sparkContext.makeRDD(Seq(
      (1L, ("小红", 20)),
      (3L, ("小明", 33)),
      (5L, ("小七", 20)),
      (7L, ("小王", 60)),
      (9L, ("小李", 20)),
      (11L, ("小美", 30)),
      (13L, ("小花", 200))
    ))


    val edge: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 3L, 0),
      Edge(3L, 9L, 0),
      Edge(13L, 3L, 0),
      Edge(5L, 7L, 0),
      Edge(11L, 9L, 0)

    ))


    val graph = Graph(point, edge)

    val vertices = graph.connectedComponents().vertices
    vertices.join(point).map({
      case (userId, (conId, (name, age))) => {
        (conId, List(name, age))
      }
    }).reduceByKey(_ ++ _)

    val hey: RDD[(Int, String)] = vertices.map(x => {
      val yushui = x._2.toString
      (x._1.toInt, yushui)
    })

    import spark.implicits._
    val frame = hey.toDF("point", "property")

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val jdbcDF = frame.write.jdbc("jdbc:mysql://localhost:3306/dadaji?useUnicode=true&amp;characterEncoding=utf-8", "graph", connectionProperties)
  }
}


