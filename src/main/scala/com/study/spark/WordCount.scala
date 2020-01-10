package com.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
     val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/wordcount/input")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wc: RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

    val result: Array[(String, Int)] = wc.collect()

    result.foreach(println)

  }

}
