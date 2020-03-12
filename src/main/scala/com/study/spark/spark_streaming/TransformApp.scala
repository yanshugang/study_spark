package com.study.spark.spark_streaming

/**
  * 实战：黑名单过滤
  * 1）transorm算子
  * 2）spark streaming整合RDD进行操作
  */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 模拟构建黑名单（一般黑名单是存储在数据库中）
    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true)) // RDD

    // 接收数据流
    val lines = ssc.socketTextStream("localhost", 9999)

    // transorm算子：应用在DStream上，可以用于执行任意的RDD到RDD的转换操作
    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD).filter(x => x._2._2.getOrElse(false) != true).map(x => x._2._1)
    })

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}


//黑名单过滤
//
//访问日志 ==> DStream
//20190506,zs
//20190506,ls
//20190506,ww
//==> (zs: 20190506,zs) (ls: 20190506,ls) (ww: 20190506,ww)
//
//黑名单列表 ==> RDD
//zs
//ls
//==>(zs: true)(ls: true)
//
//
//==> 20190506,ww
//
//leftjoin
//(zs: [<20190506,zs>, <true>])
//(ls: [<20190506,ls>, <true>])
//(ww: [<20190506,ww>, <false>])
