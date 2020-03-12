package com.study.spark.spark_project.analysis.item1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 概念解释：
  * session访问时长：一个session对应的开始的action到结束的action之间的时间范围。
  * session访问步长：一个session执行期间内，依次点击过多少个页面。
  *
  * session过滤条件：
  *
  *
  * DF版本的session统计：
  * 1. 统计访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的 session占比。
  * 2. 统计访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比。
  */
object SessionRatioDF {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SessionRatioDF")
    val spark: SparkSession = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

    // 获取数据。可修改sql语句实现对数据的过滤。
    val actionSql: String = "select * from user_visit_action where date='2020-01-16' "
    val userSql: String = "select * from user_info"
    val actionDF: DataFrame = spark.sql(actionSql)
    val userDF: DataFrame = spark.sql(userSql)

    actionDF.show(20, truncate = false)
    userDF.show(20, truncate = false)

    // 统计：每个session的时长和步长
    actionDF.createOrReplaceTempView("action")
    val sql: String =
      """
        |select user_id, session_id,
        |date_format(max(action_time), "yyyy-MM-dd'T'HH:mm:ss") as max_action_time,
        |date_format(min(action_time), "yyyy-MM-dd'T'HH:mm:ss") as min_action_time,
        |count(session_id) as sl
        |from action group by session_id, user_id
      """.stripMargin
    val ratioDF: DataFrame = spark.sql(sql).toDF()
    // TODO：sql无法实现时间秒级做差。使用df实现。
    ratioDF.show(30, truncate = false)


  }

}
