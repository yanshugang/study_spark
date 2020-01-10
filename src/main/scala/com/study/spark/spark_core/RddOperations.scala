package com.study.spark.spark_core

/**
  * RDD算子操作
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddOperations {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RddOperations")
    val sc = new SparkContext(config)

    // 初始化一个RDD
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    val listRdd2: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4), List(5, 6)))

    // map算子
    val mapRdd: RDD[Int] = listRdd.map(_ * 2)
    mapRdd.foreach(println)

    // mapPartitions算子
    val parationRdd: RDD[Int] = listRdd.mapPartitions(data => {
      data.map(_ * 2)
    })
    parationRdd.foreach(println)

    // mapPartitionsWithIndex算子
    val indexRDD: RDD[(Int, String)] = listRdd.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号：" + num))
      }
    }
    indexRDD.foreach(println)

    // flatMap算子
    val flatMapRdd: RDD[Int] = listRdd2.flatMap(data => data)
    flatMapRdd.foreach(println)

    // glom算子
    // 练习：
    val rdd1: RDD[Int] = sc.makeRDD(1 to 16, 4)
    val glomRdd: RDD[Array[Int]] = rdd1.glom()
    glomRdd.foreach { array =>
      println(array.mkString(","))
    }

    // groupBy算子
    val rdd2: RDD[Int] = sc.makeRDD(1 to 16)
    val groupByRdd: RDD[(Int, Iterable[Int])] = rdd2.groupBy(_ % 2)
    groupByRdd.foreach(println)

    // filter算子
    val rdd3: RDD[Int] = sc.makeRDD(1 to 16)
    val filterRdd: RDD[Int] = rdd3.filter(_ % 2 == 0)
    filterRdd.foreach(println)

    // sample算子
    val rdd4: RDD[Int] = sc.makeRDD(1 to 1000)
    val sampleRdd: RDD[Int] = rdd4.sample(false, 0.01, 1)
    val res4: String = sampleRdd.collect().mkString(",")
    println(res4)

    // distinct算子
    val rdd5: RDD[Int] = sc.makeRDD(Array(1, 1, 2, 2, 2, 3, 4, 5, 5))
    val distinctRdd: RDD[Int] = rdd5.distinct() // 也可以改变默认的分区数量，distinct(2)
    val res5: String = distinctRdd.collect().mkString(",")
    println(res5)

    // coalesce
    val rdd6: RDD[Int] = sc.makeRDD(1 to 32, 4)

    println("原数据集分区数" + rdd6.partitions.length)
    val coalesceRdd: RDD[Int] = rdd6.coalesce(3, shuffle = false)
    println("缩减后的分区数" + coalesceRdd.partitions.length)
    // coalesceRdd.saveAsTextFile("output")

    // repartition
    val rdd7: RDD[Int] = sc.makeRDD(1 to 32, 4)
    println("原数据集分区数" + rdd7.partitions.length)
    val repartitionRdd: RDD[Int] = rdd7.repartition(3)
    println("缩减后的分区数" + repartitionRdd.partitions.length)
    // repartitionRdd.saveAsTextFile("output1")

    // sortBy
    val rdd8: RDD[Int] = sc.makeRDD(Array(2, 5, 1, 3, 7))
    val res: Array[Int] = rdd8.sortBy(x => x).collect()
    println(res.mkString(","))

    // 双value类型
    val rdd21: RDD[Int] = sc.makeRDD(1 to 10)
    val rdd22: RDD[Int] = sc.makeRDD(5 to 14)
    // union
    val unionRdd: RDD[Int] = rdd21.union(rdd22)
    val res9: String = unionRdd.collect().mkString(",")
    println("union res: " + res9)

    // substract
    val subtractRdd: RDD[Int] = rdd21.subtract(rdd22)
    val res10: String = subtractRdd.collect().mkString(",")
    println("substract res: " + res10)

    // intersection
    val intersectionRdd: RDD[Int] = rdd21.intersection(rdd22)
    val res11: String = intersectionRdd.collect().mkString(",")
    println("intersection res: " + res11)

    // cartesian
    val cartesianRdd: RDD[(Int, Int)] = rdd21.cartesian(rdd22)
    val res12: String = cartesianRdd.collect().mkString(",")
    println("cartesian res: " + res12)

    // zip
    val zipRdd: RDD[(Int, Int)] = rdd21.zip(rdd22)
    val res13: String = zipRdd.collect().mkString(",")
    println("zip res: " + res13)

    // TODO：kv类型
    // partitionBy


  }

}
