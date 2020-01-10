package com.study.spark.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * spark处理json数据
  * 如果JSON文件中每一行就是一个JSON记录，那么可以通过将JSON文件当做文本文件来读取，然后利用相关的JSON库对每一条数据进行JSON解析。
  * 更推荐采用SparkSQL处理JSON文件。
  */
object RddReadJsonFile {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RddReadJsonFile")
    val sc = new SparkContext(config)

    val jsonData: RDD[String] = sc.textFile("data/test.json")
    val result: RDD[Option[Any]] = jsonData.map(JSON.parseFull)
    result.collect().foreach(println)
  }

}
