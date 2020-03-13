package com.study.spark.spark_sql.models

/**
  *
  * @param dateid    日期
  * @param years     年月
  * @param theyear   年
  * @param month     月
  * @param day       日
  * @param weekday   周几
  * @param week      第几周
  * @param quarter   季度
  * @param period    旬
  * @param halfmonth 半月
  */
case class TBDate(dateid: String,
                  years: Int,
                  theyear: Int,
                  month: Int,
                  day: Int,
                  weekday: Int,
                  week: Int,
                  quarter: Int,
                  period: Int,
                  halfmonth: Int)
