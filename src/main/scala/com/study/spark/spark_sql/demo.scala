package com.study.spark.spark_sql

import com.study.spark.spark_sql.models.{TBDate, TBStock, TBStockDetail}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * spark-sql练习
  *
  */


object demo {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo")
    val spark: SparkSession = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
    import spark.implicits._

    // 加载数据
    val tbStockRdd: RDD[String] = spark.sparkContext.textFile("data/spark-sql/tbStock.txt")
    val tbStockDS: Dataset[TBStock] = tbStockRdd.map(_.split(",")).map(attr => TBStock(attr(0), attr(1), attr(2))).toDS()

    val tbStockDetailRdd: RDD[String] = spark.sparkContext.textFile("data/spark-sql/tbStockDetail.txt")
    val tbStockDetailDS: Dataset[TBStockDetail] = tbStockDetailRdd.map(_.split(",")).map(attr => TBStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS

    val tbDateRdd: RDD[String] = spark.sparkContext.textFile("data/spark-sql/tbDate.txt")
    val tbDateDS: Dataset[TBDate] = tbDateRdd.map(_.split(",")).map(attr => TBDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS

    tbStockDS.show()
    tbStockDetailDS.show()
    tbDateDS.show()

    // 注册表
    tbStockDS.createOrReplaceTempView("tbStock")
    tbDateDS.createOrReplaceTempView("tbDate")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    // 计算所有订单中每年的销售单数、销售总额
    val sql_1: String =
      """
        |select c.theyear, count(c.ordernumber), sum(d.amount)
        |from (
        |select a.ordernumber, b.theyear
        |from tbStock a
        |join tbDate b
        |on a.dateid = b.dateid) c
        |join tbStockDetail d
        |on c.ordernumber = d.ordernumber
        |group by c.theyear
        |order by c.theyear
      """.stripMargin
    val res_1: DataFrame = spark.sql(sql_1)
    res_1.show()


    // 计算所有订单每年最大金额订单的销售额
    // 计算所有订单中每年最畅销货品
  }

}
