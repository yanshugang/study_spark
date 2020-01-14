package com.study.spark.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}


object Transform2 {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform2")
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()
    import spark.implicits._

    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(Array((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    // 将rdd直接转换为DS
    val userRdd: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDs: Dataset[User] = userRdd.toDS()

    // ds转rdd
    val rdd_1: RDD[User] = userDs.rdd
    rdd_1.foreach(println)

  }

}
