package com.study.spark.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SqlApi {

  def main(args: Array[String]): Unit = {

    // 初始化spark
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SqlApi")
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()


    val file_path: String = "/Users/yanshugang/Desktop/test.json"
    val df: DataFrame = spark.read.json(file_path)

    // 定义临时视图
    df.createOrReplaceTempView("test")
    // 定义临时全局视图
    // df.createGlobalTempView("test_global")

    // 对临时视图可使用sql语句
    val sqlDf: DataFrame = spark.sql("select * from test")


    spark.stop()
  }

}
