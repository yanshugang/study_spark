package com.study.spark.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DF操作：
  * DF操作：很少用，一般会转换成sql操作
  * 读写：HDFS/S3/MySQL/HIIVE
  */
object DFOperations {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DFOperations")
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val df: DataFrame = spark.read.format("json").load("data/test.json")


    println("===== %s =====".format("DF操作-schema信息：printSchema"))
    df.printSchema()

    println("===== %s =====".format("DF操作-展示：show"))
    df.show(2, truncate = false) // 展示2条数据，不截断

    println("===== %s =====".format("DF操作-查找：select"))
    df.select("name", "age").show()
    df.select(df.col("name"), (df.col("age") + 10).as("age2")).show() // 可以使用as重命名

    println("===== %s =====".format("DF操作-过滤：filter"))
    df.filter(df.col("age") > 19).show()
    df.filter(df.col("age") === 13).show()

    println("===== %s =====".format("DF操作-聚合：groupby"))
    // 按照年龄聚合统计出现次数
    df.groupBy("age").count().show()

    println("===== %s =====".format("DF操作-关联：join"))
    val df_2: DataFrame = spark.read.format("json").load("data/test_2.json")
    val join_df: DataFrame = df.join(df_2, df.col("name") === df_2.col("name"))
      .select(df.col("name"), df.col("age"), df_2.col("sex"))
    join_df.show()


    // TODO: spark读取HDFS、S3、MySQL、Hive
    // TODO: spark保存DF到：HDFS、S3、MySQL、Hive


    spark.stop()
  }

}
