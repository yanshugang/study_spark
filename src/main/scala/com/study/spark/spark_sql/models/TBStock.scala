package com.study.spark.spark_sql.models

/**
  *
  * @param ordernumber 订单号
  * @param locationid 交易位置
  * @param dateid 交易日期
  */
case class TBStock(ordernumber: String,
                   locationid: String,
                   dateid: String)
