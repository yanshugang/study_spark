package com.study.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用saprk streaming完成有状态的统计：updateStateByKey
  */
object StatefulWordCount {

  def updateFunction(values: Seq[Int], state: Option[Int]): Option[Int] = {
    var newValues = state.getOrElse(0)
    for (value <- values) {
      newValues += value
    }
    Option(newValues)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 如果使用stateful算子，必须要设置checkpoint，在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint("./checkpoint/")

    val lines = ssc.socketTextStream("localhost", 9999)

    val result = lines.flatMap(_.split(" ")).map((_, 1))
    val state = result.updateStateByKey[Int](updateFunction(_, _))

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
