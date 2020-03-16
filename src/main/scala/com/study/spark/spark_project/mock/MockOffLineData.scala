package com.study.spark.spark_project.mock

import java.util.UUID

import com.study.spark.spark_project.commons.models.{ProductInfo, UserInfo, UserVisitAction}
import com.study.spark.spark_project.commons.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 模拟日志生成，存储到Hive，用于离线数据分析。
  */
object MockOffLineData {

  /**
    * 模拟用户行为信息
    */
  private def mockUserVisitActionData(): Array[UserVisitAction] = {

    val rows: ArrayBuffer[UserVisitAction] = ArrayBuffer[UserVisitAction]()

    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")
    // yyyy-MM-dd
    val date: String = DateUtils.getTodayDate()
    // 关注四个行为：搜索、点击、下单、支付
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()

    // 一共100个用户（有重复）
    for (i <- 0 to 1000) {
      val userid: Int = random.nextInt(1000)
      // 每个用户产生10个session
      for (j <- 0 to 10) {
        // 不可变的，全局的，独一无二的128bit长度的标识符，用于标识一个session，体现一次会话产生的sessionId是独一无二的
        val sessionid: String = UUID.randomUUID().toString().replace("-", "")
        // 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
        val baseActionTime: String = date + " " + random.nextInt(23)
        // 每个(userid + sessionid)生成0-100条用户访问数据
        for (k <- 0 to random.nextInt(100)) {
          val pageid: Int = random.nextInt(10)
          // 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
          val actionTime: String = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid: Long = random.nextInt(10).toLong
          // 随机确定用户在当前session中的行为
          val action: String = actions(random.nextInt(4))

          // 根据随机产生的用户行为action决定对应字段的值
          action match {
            case "search" =>
              searchKeyword = searchKeywords(random.nextInt(10))
            case "click" =>
              clickCategoryId = random.nextInt(100).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" =>
              orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" =>
              payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid, pageid, actionTime, searchKeyword, clickCategoryId,
            clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityid)
        }
      }
    }

    rows.toArray
  }

  /**
    * 模拟用户信息
    */
  private def mockUserInfoData(): Array[UserInfo] = {
    val rows: ArrayBuffer[UserInfo] = ArrayBuffer[UserInfo]()

    val sexes = Array("male", "female")
    val random = new Random()

    // 随机产生100个用户的个人信息
    for (i <- 0 to 1000) {
      val userid: Int = i
      val username: String = "user" + i
      val name: String = "name" + i
      val age: Int = random.nextInt(60)
      val professional: String = "professional" + random.nextInt(100)
      val city: String = "city" + random.nextInt(100)
      val sex: String = sexes(random.nextInt(2))

      rows += UserInfo(userid, username, name, age, professional, city, sex)
    }

    rows.toArray
  }

  /**
    * 模拟产品信息
    */
  private def mockProdectInfoData(): Array[ProductInfo] = {

    val rows: ArrayBuffer[ProductInfo] = ArrayBuffer[ProductInfo]()

    val random = new Random()
    val productStatus = Array(0, 1)

    // 随机产生100个产品信息
    for (i <- 0 to 100) {
      val productId: Int = i
      val productName: String = "product" + i
      val extendInfo: String = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }

  /**
    * 将DF写到Hive
    */
  private def insertHive(spark: SparkSession, tableName: String, dateDF: DataFrame): Unit = {
    spark.sql("drop table if exists " + tableName)
    dateDF.write.saveAsTable(tableName)

  }

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MockOffLineData")
    val spark: SparkSession = SparkSession.builder()
      .config(config)
      .enableHiveSupport()
      .getOrCreate()

    // 模拟数据
    val userVisitActionData: Array[UserVisitAction] = this.mockUserVisitActionData()
    val userInfoData: Array[UserInfo] = this.mockUserInfoData()
    val productInfoData: Array[ProductInfo] = this.mockProdectInfoData()

    // Array转DF
    import spark.implicits._
    val userVisitActionDF: DataFrame = spark.sparkContext.makeRDD(userVisitActionData).toDF()
    val userInfoDF: DataFrame = spark.sparkContext.makeRDD(userInfoData).toDF()
    val productInfoDF: DataFrame = spark.sparkContext.makeRDD(productInfoData).toDF()

    // 写到Hive
    insertHive(spark, "user_visit_action", userVisitActionDF)
    insertHive(spark, "user_info", userInfoDF)
    insertHive(spark, "product_info", productInfoDF)

    spark.close()

  }

}
