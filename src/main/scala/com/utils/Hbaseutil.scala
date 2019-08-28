package com.utils
object Hbaseutil {
  def main(args: Array[String]): Unit = {
  }
}

//    //创建上下文
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//
//    //todo 调用hbase api
//    //加载hbase配置文件
//    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.TableName")
//    //创造hadoop任务
//    val configuration = sc.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))
//    //创建hbaseconnection
//    val hbconn = ConnectionFactory.createConnection(configuration)
//    val hbadmin = hbconn.getAdmin
//    //判断表是否存在可用
//    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
//      //创建表操作
//      val tablesDecription = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      val descriptor = new HColumnDescriptor("tags")
//      tablesDecription.addFamily(descriptor)
//      hbadmin.createTable(tablesDecription)
//      hbadmin.close()
//      hbconn.close()
//    }
//    //创建JobConf
//    val jobConf = new JobConf(configuration)
//    //指定输出类型和表
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    //      .map{
    //        case(userid,userTag)=>{
    //          val put = new Put(Bytes.toBytes(userid))
    //          //处理一下标签
    //          val tags = userTag.map(t=>t._1+""+t._2).mkString(",")
    //          put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
    //          (new ImmutableBytesWritable(),put)
    //        }
    //      }
    //      .saveAsHadoopDataset(jobConf)


