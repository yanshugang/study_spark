package com.study.spark.spark_streaming

/**
  * 算子：foreachRDD
  * 使用saprk streaming完成词频统计，并将结果写到mysql。
  */

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp {

  // 获取mysql的链接
  def CreateConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project?useSSL=false", "root", "yanshugang1019")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    // 插入数据库
    result.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val connection = CreateConnection()  // 每个partition创建一个链接
        partitionOfRecords.foreach { record =>
          val word = record._1
          val word_count = record._2
          val sql = s"insert into wordcount(word, word_count) values('$word', '$word_count');"
          connection.createStatement().execute(sql)
        }
        connection.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

/*
改进：
  1）对于已有的数据做更新，而是所有的数据均为insert
      改进思路：
          a) 在插入前先判断单词是否存在，如果存在就update，不存在就insert
          b) 实际业务中，使用HBase或redis
  2）每个rdd的partition创建connection，建议改成连接池
 */
