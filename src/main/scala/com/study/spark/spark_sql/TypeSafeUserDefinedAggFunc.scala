package com.study.spark.spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * 强类型自定义聚合函数，练习：求用户年龄的平均值
  */

object TypeSafeUserDefinedAggFunc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("TypeSafeUserDefinedAggFunc").master("local[*]").getOrCreate()

    val file_path = "data/test.json"
    val userDF: DataFrame = spark.read.json(file_path)
    import spark.implicits._
    val userDS: Dataset[UserBean] = userDF.as[UserBean]

    // 创建聚合函数对象
    val udaf = new TypeSafeUserDefinedAggFunc

    // 将聚合函数转换为查询列
    val avgAge: TypedColumn[UserBean, Double] = udaf.toColumn.name("avg_age")
    val result: Dataset[Double] = userDS.select(avgAge)
    result.show()

  }

}


// 输入的数据样例类
case class UserBean(name: String, age: Long)

// 缓冲区计算时的数据样例类
case class AvgBuffer(var sum: Long, var count: Long)

class TypeSafeUserDefinedAggFunc extends Aggregator[UserBean, AvgBuffer, Double] {
  // 缓冲区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L, 0L)
  }

  // 更新缓冲区数据
  override def reduce(buffer: AvgBuffer, employee: UserBean): AvgBuffer = {
    buffer.sum += employee.age
    buffer.count += 1
    buffer
  }

  // 合并多个节点的缓冲区
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 最终计算逻辑
  override def finish(reduction: AvgBuffer): Double = reduction.sum.toDouble / reduction.count

  // 转码，一般缓冲区的设为Encoders.product
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  // 转码，一般自定义的数据结构设为Encoders.scalaDouble
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}
