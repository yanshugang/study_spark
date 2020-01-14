package com.study.spark.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD\DF\DS三者间的转换
  */


case class User(id: Int, name: String, age: Int)

object Transform {

  def main(args: Array[String]): Unit = {
    // 配置信息
    val config: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Transform")

    // 初始化spark
    val spark: SparkSession = SparkSession.builder()
      .config(config)
      .getOrCreate()

    // 创建RDD
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(Array((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    import spark.implicits._
    // RDD转DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    // DF转DS
    val ds: Dataset[User] = df.as[User]

    // DS转DF
    val df_1: DataFrame = ds.toDF()

    // DF转RDD
    val rdd_1: RDD[Row] = df_1.rdd

    rdd_1.foreach { row =>
      // 可以通过索引访问row的数据
      println(row.getString(1))
    }

    // 释放资源
    spark.stop()
  }

}
