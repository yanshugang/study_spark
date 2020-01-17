package com.study.spark.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器
  */
object Acc {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShareData")
    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    // val sum: Int = dataRDD.reduce(_ + _)
    // println(sum)

    // var sum: Int = 0
    // dataRDD.foreach(i => sum = sum + i)
    // println(sum)  // 结果为0. 因为在excutor计算完的结果并没有回传到driver端

    // 使用累加器来累加数据
    // step-1: 创建累加器对象
    val acc: LongAccumulator = sc.longAccumulator
    dataRDD.foreach(i => acc.add(i))
    println(acc.value)

    // 使用自定义累加器
    // step-1: 创建累加器
    val myAcc = new MyAcc
    // step-2: 注册累加器
    sc.register(myAcc, "word acc")
    // step-3: 使用
    val wordRDD:RDD[String] = sc.makeRDD(Array("hadoop", "spark", "flink", "hbase"))
    wordRDD.foreach(word => myAcc.add(word))
    println(myAcc.value)

    sc.stop()
  }

}
