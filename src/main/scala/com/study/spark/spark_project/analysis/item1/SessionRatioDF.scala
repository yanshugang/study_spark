package com.study.spark.spark_project.analysis.item1

import java.text.SimpleDateFormat
import java.util.Date

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

  /**
    * 时间做差
    *
    * @param startTime session的开始时间
    * @param endTime   session的结束时间
    * @return
    */
  def getVisitLength(startTime: String, endTime: String): String = {
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val maxTime: Date = dataFormat.parse(endTime)
    val minTime: Date = dataFormat.parse(startTime)
    val between: Long = (maxTime.getTime - minTime.getTime) / 1000
    between.toString
  }

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SessionRatioDF")
    val spark: SparkSession = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

    // 获取用户行为数据。
    val actionSql: String = "select * from user_visit_action where date='2020-03-15' "
    val actionDF: DataFrame = spark.sql(actionSql)

    // 按照session聚合数据
    // session_id\search_keywords\click_category_id\visit_length\step_length\start_time
    actionDF.createOrReplaceTempView("action")

    // 自定义函数
    spark.udf.register("visitLenthSecond", (startTime: String, endTime: String) => getVisitLength(startTime, endTime))

    val sql: String =
      """
        |select user_id, session_id,
        |concat_ws("|", collect_set(search_keyword)) as search_keywords,
        |concat_ws("|", collect_set(click_category_id)) as click_category_ids,
        |visitLenthSecond(min(action_time), max(action_time)) as visit_length,
        |count(session_id) as step_length,
        |min(action_time) as start_time
        |from action
        |group by session_id, user_id
      """.stripMargin
    val ratioDF: DataFrame = spark.sql(sql)

    // 获取用户信息数据
    val userSql: String = "select * from user_info"
    val userDF: DataFrame = spark.sql(userSql)

    // 联立user_info表
    val joinDF: DataFrame = ratioDF.join(userDF, ratioDF("user_id") === userDF("user_id"), "left_outer")
      .select(ratioDF("session_id"), ratioDF("search_keywords"), ratioDF("click_category_ids"),
        ratioDF("visit_length"), ratioDF("step_length"), ratioDF("start_time"), userDF("age"),
        userDF("professional"), userDF("sex"), userDF("city"))
    joinDF.show(20)

    // 根据条件过滤数据: 主要是针对age\professional\sex\city

    // 统计
    // 1. 统计访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的 session占比。
    // 2. 统计访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比。

    joinDF.createOrReplaceTempView("full_info")
    val sql_1: String =
      """
        |select
        |session_id,
        |case
        |when(visit_length>=1 and visit_length<=3) then "1s~3s"
        |when(visit_length>=4 and visit_length<=6) then "4s~6s"
        |when(visit_length>=7 and visit_length<=9) then "7s~9s"
        |when(visit_length>=10 and visit_length<30) then "10s~30s"
        |when(visit_length>=30 and visit_length<60) then "30s~60s"
        |when(visit_length>=60 and visit_length<180) then "1m~3m"
        |when(visit_length>=180 and visit_length<600) then "3m~10m"
        |when(visit_length>=600 and visit_length<1800) then "10m~30m"
        |else "30m以上"
        |end as visit_length_tag,
        |case
        |when(step_length>=1 and step_length<=3) then "1~3"
        |when(step_length>=4 and step_length<=6) then "4~6"
        |when(step_length>=7 and step_length<=9) then "7~9"
        |when(step_length>=10 and step_length<30) then "10~30"
        |when(step_length>=30 and step_length<60) then "30~60"
        |else "60以上"
        |end as step_length_tag
        |from full_info
      """.stripMargin
    val tagDF: DataFrame = spark.sql(sql_1)

    tagDF.show(100, truncate = false)


    // 统计比例
    tagDF.createOrReplaceTempView("tag_table")
    val ratio_sql_1: String =
      """
        |select
        |visit_length_tag,
        |count(1) as tag_num
        |from tag_table
        |group by visit_length_tag
        |order by visit_length_tag
      """.stripMargin
    val resDF1: DataFrame = spark.sql(ratio_sql_1)
    val ratio_sql_2: String =
      """
        |select
        |step_length_tag,
        |count(1) as tag_num
        |from tag_table
        |group by step_length_tag
        |order by step_length_tag
      """.stripMargin
    val resDF2: DataFrame = spark.sql(ratio_sql_2)

    resDF1.show()
    resDF2.show()

  }

}
