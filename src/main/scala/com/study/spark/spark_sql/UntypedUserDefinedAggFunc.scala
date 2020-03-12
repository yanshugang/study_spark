package com.study.spark.spark_sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 弱类型自定义聚合函数
  * 练习：求用户年龄的平均值
  *
  * 弱类型的UDAF缺点：缓冲区的数据是以数组形式存储，容易记混。
  */
object UntypedUserDefinedAggFunc extends UserDefinedAggregateFunction {

  // 输入的数据结构
  override def inputSchema: StructType = new StructType().add("age", LongType)

  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 输出时的数据类型
  override def dataType: DataType = DoubleType

  // 函数稳定性
  override def deterministic: Boolean = true

  // 计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      // sum
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      // count
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算逻辑
  override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("UntypedUserDefinedAggFunc")
      .master("local[*]")
      .getOrCreate()

    // 注册函数
    spark.udf.register("avgAge", UntypedUserDefinedAggFunc)

    val file_path = "data/test.json"
    val df: DataFrame = spark.read.json(file_path)

    df.createOrReplaceTempView("tmp")

    val result: DataFrame = spark.sql("select avgAge(age) from tmp")

    result.show()

  }


}
