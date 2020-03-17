package com.study.spark.spark_project.analysis.item1

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 概念解释：
  * session访问时长：一个session对应的开始的action到结束的action之间的时间范围。
  * session访问步长：一个session执行期间内，依次点击过多少个页面。
  *
  * session过滤条件：
  *
  *
  * 需求一：session统计：
  * *** 1. 统计访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的 session占比。
  * *** 2. 统计访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比。
  *
  * 需求二：按时间比例随机抽取100个session：
  * *** step-1：统计每个小时session占比，计算出每个小时应该抽取几个session
  * *** step-2：
  *
  * 需求三：TOP10热门商品统计：
  *
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

  def getHour(t: String): String = {
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time: Date = dataFormat.parse(t)
    time.getHours.toString
  }

  def sessionRatio(spark: SparkSession, df: DataFrame): Unit = {
    // 划分区间
    df.createOrReplaceTempView("full_info")
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

    // 统计占比
    tagDF.createOrReplaceTempView("tag_table")
    // 访问时长统计占比
    val visitLengthRatioSql: String =
      """
        |select visit_length_tag,
        |round(count(visit_length_tag)/(select count(session_id) from tag_table), 2 )as visit_length_ratio
        |from tag_table
        |group by visit_length_tag
      """.stripMargin
    val visitLengthRatioDF: DataFrame = spark.sql(visitLengthRatioSql)

    // 统计访问步长
    val stepLengthRatioSql: String =
      """
        |select step_length_tag,
        |round(count(step_length_tag)/(select count(session_id) from tag_table), 2) as step_length_ratio
        |from tag_table
        |group by step_length_tag
      """.stripMargin
    val stepLengthRatioDF: DataFrame = spark.sql(stepLengthRatioSql)

    // 统计结果展示
    visitLengthRatioDF.show()
    stepLengthRatioDF.show()

    // 使用df.write写入MySQL
    visitLengthRatioDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/commerce?useUnicode=true&characterEncoding=utf8&useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "yanshugang1019")
      .option("dbtable", "visit_length_ratio_20200317")
      .mode(SaveMode.Append)
      .save()

    stepLengthRatioDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/commerce?useUnicode=true&characterEncoding=utf8&useSSL=false")
      .option("user", "root")
      .option("password", "yanshugang1019")
      .option("dbtable", "step_length_ratio_20200317")
      .mode(SaveMode.Append)
      .save()


  }

  def sessionRandomExtract(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("full_info")
    spark.udf.register("getHour", (time: String) => getHour(time))

    // 计算每个小时要抽取的session数量: （该小时的session数量/该天的session数量）* 一天要抽取的session数量
    val sql: String =
      """
        |select
        |getHour(start_time),
        |round(count(1)/(select count(1) from full_info), 2) as hour_ratio
        |from full_info
        |group by getHour(start_time)
        |order by getHour(start_time)
      """.stripMargin

    spark.sql(sql).show()

  }

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SessionRatioDF")
    val spark: SparkSession = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

    // 获取用户行为数据。
    val actionSql: String = "select * from user_visit_action where date='2020-03-17' "
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

    joinDF.cache()

    // 根据条件过滤数据: 主要是针对age\professional\sex\city

    // 统计
    // 1. 统计访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的 session占比。
    // 2. 统计访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比。


    // 需求一：统计访问时长占比、访问步长占比
//    sessionRatio(spark, joinDF)

    // 需求二：随机抽取session
    sessionRandomExtract(spark, joinDF)


    spark.stop()


  }

}
