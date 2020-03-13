package com.study.spark.spark_sql.models

/**
  *
  * @param ordernumber 订单号
  * @param rownum      行号
  * @param itemid      货号
  * @param number      数量
  * @param price       单价
  * @param amount      销售额
  */
case class TBStockDetail(ordernumber: String,
                         rownum: Int,
                         itemid: String,
                         number: Int,
                         price: Double,
                         amount: Double)
