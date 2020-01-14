package com.study.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming有状态操作
  */
object UpdateState {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UpdateState")
    val ssc = new StreamingContext(config, Seconds(5))

    // 将数据状态保存到文件
    ssc.sparkContext.setCheckpointDir("cp")

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "localhost:2181", "spark", Map("source" -> 3))

    val wordOne: DStream[(String, Int)] = kafkaDStream.flatMap(kafkaMsg => kafkaMsg._2.split(" ")).map((_, 1))

    // TODO: updateStateByKey的用法
    val stateDStream: DStream[(String, Int)] = wordOne.updateStateByKey {
      case (seq, buffer) => {
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    stateDStream.print()



    ssc.start()
    ssc.awaitTermination()
  }

}
