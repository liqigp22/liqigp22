package Text8_24

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LiqiContext {
  /**
    *
    *
    *
    */


  //
  def main(args: Array[String]): Unit = {
      if(args.length != 1){
        println( "目录不匹配，退出程序"  )
        sys.exit()
      }
      val Array(inputpath1 ) = args


      val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

      //读取文件
       val inp = spark.read.textFile(inputpath1).rdd

     //输出聚合的Bus结果，第一题的结果


    val res: RDD[(String, Int)] = inp.map(jsonstr => {
      val bus = BusUtils.getBusinessFromap(jsonstr)
      (bus, 1)
    })
    val resS: RDD[Array[String]] = res.map(x => {
      val strings: Array[String] = x._1.split(",")
      strings
    })
    val resN = resS.flatMap(x => x).map(x => (x, 1))
    resN.reduceByKey(_+_).foreach(println)
    //res.reduceByKey(_+_).foreach(println)


      println("----------------------------------------------------------")

    val typ: RDD[(String, Int)] = inp.map(jsonstr => {
       val typ = Type.getTypeFromap(jsonstr)
      (typ, 1)
    })

    val types: RDD[Array[String]] = typ.map(x => {
      val typstr: Array[String] = x._1.split(",")
        typstr
    })
   val typN = types.flatMap(x => x).map(x => (x, 1))
    //typN.reduceByKey(_+_).foreach(println)
    val Nametye: RDD[Array[String]] = typN.map(x => {
      val dadaji = x._1.split(";")
      dadaji
    })
    Nametye.flatMap(x=>{
      x
    }).map((_,1)).reduceByKey(_+_).foreach(println)

  }
}
