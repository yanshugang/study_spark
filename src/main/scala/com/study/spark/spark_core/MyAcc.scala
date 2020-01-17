package com.study.spark.spark_core

import java.util
import org.apache.spark.util.AccumulatorV2

/**
  * 自定义累加器
  */
class MyAcc extends AccumulatorV2[String, util.ArrayList[String]] {

  private val list = new util.ArrayList[String]()

  // 当前累加器是否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new MyAcc
  }

  // 重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中增加数据
  override def add(v: String) = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  // 合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的值
  override def value: util.ArrayList[String] = {
    list
  }
}