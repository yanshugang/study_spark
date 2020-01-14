package com.study.spark.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SqlApi {

  def main(args: Array[String]): Unit = {

    // 配置信息
    val config: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Demo")

    // 初始化spark
    val spark: SparkSession = SparkSession.builder()
      .config(config)
      .getOrCreate()


    val file_path: String = "/Users/yanshugang/Desktop/test.json"
    val df: DataFrame = spark.read.json(file_path)

    df.createOrReplaceTempView("test")

    val sqlDf: DataFrame = spark.sql("select * from test")


    spark.stop()
  }

}
