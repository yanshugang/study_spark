package com.study.spark.spark_sql

import com.study.spark.spark_sql.models.User
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD\DF\DS三者间的转换:
  * RDD -> DF
  * DF -> DS
  * DS -> DF
  * DF -> RDD
  */

object Transform {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(Array((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40), (4, "zhaoliu", 3)))

    // RDD -> DF TODO:[传说中的两种方式：编程、反射]
    val df: DataFrame = rdd.toDF("id", "name", "age") // 一般直接使用反射转换的方式

    // RDD -> DS
    val userRdd: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDs: Dataset[User] = userRdd.toDS()

    // DF转DS
    val ds: Dataset[User] = df.as[User]

    // DS转DF
    val df_1: DataFrame = ds.toDF()

    // DF转RDD
    val rdd_1: RDD[Row] = df_1.rdd

    // DS -> RDD
    val rdd_2: RDD[User] = userDs.rdd

    rdd_1.foreach { row =>
      // 可以通过索引访问row的数据
      println(row.getString(1))
    }

    // 释放资源
    spark.stop()
  }

}
