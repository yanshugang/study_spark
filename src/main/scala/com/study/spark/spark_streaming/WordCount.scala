package com.study.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark streaming完成word count
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc = new StreamingContext(config, Seconds(5))

    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))

    val wordAndOneStreams: DStream[(String, Int)] = wordStreams.map((_, 1))
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    wordAndCountStreams.print()

    // 启动采集器
    ssc.start()
    // driver等待采集器执行
    ssc.awaitTermination()


  }

}
