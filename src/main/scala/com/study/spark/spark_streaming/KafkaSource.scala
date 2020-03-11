package com.study.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 消费kafka数据
  */
object KafkaSource {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSource")
    val ssc = new StreamingContext(config, Seconds(5))

    // 初始化kafka接收器。TODO：优化传参
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "localhost:2181", "spark", Map("source" -> 3))

    val wordCount: DStream[(String, Int)] = kafkaDStream.flatMap(kafkaMsg => kafkaMsg._2.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()


    ssc.start()
    ssc.awaitTermination()


  }

}
