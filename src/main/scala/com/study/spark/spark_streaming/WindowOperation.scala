package com.study.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowOperation {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WindowOperation")
    val ssc = new StreamingContext(config, Seconds(3))

    // 将数据状态保存到文件
    ssc.sparkContext.setCheckpointDir("cp")

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "localhost:2181", "spark", Map("source" -> 3))

    // 定义窗口。窗口大小必须是采集周期的整数倍，窗口滑动步长也必须是采集周期的整数倍
    // 建议：使用window算子定义窗口，复杂逻辑后面自己实现。不建议使用复杂的窗口算子。
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    val wordCount: DStream[(String, Int)] = windowDStream.flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCount.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
