package com.study.spark.spark_project.analysis.item1

import java.util.{Date, UUID}

import com.study.spark.spark_project.commons.constants.Constants
import com.study.spark.spark_project.commons.models.{SessionAggrStat, UserInfo, UserVisitAction}
import com.study.spark.spark_project.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

/**
  * 需求一：各个范围Session步长、访问时长占比统计。
  * 统计出符合筛选条件的session中，
  * 访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m，
  * 访问步长在1_3、4_6、…以上各个范围内的各种session的占比。
  */
object SessionRatio {

  def getOriActionRDD(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    // 使用参数工具类提取数据限制条件
    val startDate: String = taskParam.getString("startDate")
    val endDate: String = taskParam.getString("endDate")

    // sql
    val sql: String = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"

    import spark.implicits._
    // df 转 ds 转 rdd
    spark.sql(sql).as[UserVisitAction].rdd

  }

  /**
    * 聚合一个session的信息：session_id | search_keywords | click_category_ids | visit_length | step_length | stort_time
    *
    */
  def getSessionFullInfo(spark: SparkSession, sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    val userId2AggrInfoRDD: RDD[(Long, String)] = sessionId2GroupActionRDD.map {
      case (sessionId, iterableAction) => {
        var userID: Long = -1L
        var startTime: Date = null // 该session的开始时间
        var endTime: Date = null // 该session的离开时间
        var stepLength = 0 // session步长
        val searchKeywords = new StringBuffer() // 该session期间查询的所有词
        val clickCategoryIds = new StringBuffer() // 该session期间点击的所有品类

        // 遍历一个session下的全部用户行为数据
        for (action <- iterableAction) {
          // userID
          if (userID == -1L) {
            userID = action.user_id
          }

          // 使用action_time计算startTime和endTime
          val actionTime: Date = DateUtils.parseTime(action.action_time) // String转Date
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          // searchKeywords
          val searchKeyword: String = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          // clickCategoryIds
          val clickCategoryId: Long = action.click_category_id
          if (clickCategoryId != -1 && !clickCategoryIds.toString.contains(clickCategoryId)) {
            clickCategoryIds.append(clickCategoryId + ",")
          }

          // stepLength
          stepLength += 1

        }

        // 处理searchKeywords和clickCategoryIds结尾处的逗号
        val searchKw: String = StringUtils.trimComma(searchKeywords.toString)
        val clickCg: String = StringUtils.trimComma(clickCategoryIds.toString)

        // visitLength
        val visitLength: Long = (endTime.getTime - startTime.getTime) / 1000

        // 合并数据
        val aggrInfo: String = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime)

        // 因为下一步需要关联用户表，所以用userID做key
        (userID, aggrInfo)
      }
    }


    // 根据userID关联用户表：添加age、professional、sex、city四个字段
    // 获取用户信息数据
    val sql = "select * from user_info"
    import spark.implicits._
    val userID2InfoRDD: RDD[(Long, UserInfo)] = spark.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    // 两个RDD做join操作
    val sessionId2FullInfoRDD: RDD[(String, String)] = userId2AggrInfoRDD.join(userID2InfoRDD).map {
      case (userID, (aggrInfo, userInfo)) => {
        // 获取要新增的字段
        val age: Int = userInfo.age
        val professional: String = userInfo.professional
        val sex: String = userInfo.sex
        val city: String = userInfo.city

        val fullInfo: String = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city + "|"

        val sessionId: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)

      }
    }
    sessionId2FullInfoRDD

  }

  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionAcc: SessionAccumulator) = {

    // 获取筛选条件
    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 将所有筛选条件拼接成一个字符串
    var filterInfo: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")
    // 去除字符串结尾处的|
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    // 使用验证工具类判断数据是否符合筛选条件
    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }
        if (success) {
          // 符合条件的session个数
          sessionAcc.add(Constants.SESSION_COUNT)
          // 获取visitLength和stepLength的值
          val visitLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          // 根据需求更新累加器，记录不同时长、不同步长的session个数。
          calculateVisitLength(visitLength, sessionAcc)
          calculateStepLength(stepLength, sessionAcc)
        }

        success
    }

  }

  def getSessionRatio(spark: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]) = {
    // 从累加器获取值
    val session_count: Double = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s: Double = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0).toDouble
    val visit_length_4s_6s: Double = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0).toDouble
    val visit_length_7s_9s: Double = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0).toDouble
    val visit_length_10s_30s: Double = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0).toDouble
    val visit_length_30s_60s: Double = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0).toDouble
    val visit_length_1m_3m: Double = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0).toDouble
    val visit_length_3m_10m: Double = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0).toDouble
    val visit_length_10m_30m: Double = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0).toDouble
    val visit_length_30m: Double = value.getOrElse(Constants.TIME_PERIOD_30m, 0).toDouble

    val step_length_1_3: Double = value.getOrElse(Constants.STEP_PERIOD_1_3, 0).toDouble
    val step_length_4_6: Double = value.getOrElse(Constants.STEP_PERIOD_4_6, 0).toDouble
    val step_length_7_9: Double = value.getOrElse(Constants.STEP_PERIOD_7_9, 0).toDouble
    val step_length_10_30: Double = value.getOrElse(Constants.STEP_PERIOD_10_30, 0).toDouble
    val step_length_30_60: Double = value.getOrElse(Constants.STEP_PERIOD_30_60, 0).toDouble
    val step_length_60: Double = value.getOrElse(Constants.STEP_PERIOD_60, 0).toDouble

    // 求占比，保留两位有效数字
    val visit_length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio: Double = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio: Double = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio: Double = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio: Double = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio: Double = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio: Double = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio: Double = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 将全部数据封装成一个case class
    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    // 将case class转成RDD，再转成DF
    import spark.implicits._
    val statDF: DataFrame = spark.sparkContext.makeRDD(Array(stat)).toDF

    // 使用df.write写入MySQL
    statDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/commerce?useUnicode=true&characterEncoding=utf8&useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "yanshugang1019")
      .option("dbtable", "session_stat_ratio_20200116")
      .mode(SaveMode.Append)
      .save()

  }

  def main(args: Array[String]): Unit = {

    // 获取筛选条件。 TODO: ConfigurationManager异常
    // val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val jsonStr: String =
    """
      |{startDate:"2020-01-16",
      |  endDate:"2020-01-16",
      |  startAge: 20,
      |  endAge: 50,
      |  professionals: "",
      |  cities: "",
      |  sex:"",
      |  keywords:"",
      |  categoryIds:"",
      |  targetPageFlow:"1,2,3,4,5,6,7"}
    """.stripMargin
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)


    // 作为本次统计任务的唯一键，在写入MYSQL时作为主键。
    val taskUUID: String = UUID.randomUUID().toString

    // 初始化spark
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Item1SessionStat")
    val spark: SparkSession = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

    // step-1: 获取数据
    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(spark, taskParam)

    // step-2: 根据session聚合数据。以sessionId为key，value是一个ActionRDD组成的可迭代对象。RDD[sessionId, iterable_UserVisitAction]
    val sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.map(item => (item.session_id, item)).groupByKey()

    sessionId2GroupActionRDD.cache()

    // step-3: 聚合用户行为信息，然后关联用户表
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(spark, sessionId2GroupActionRDD)

    // step-4:
    // 初始化一个自定义的累加器
    val sessionAcc = new SessionAccumulator
    // 注册累加器
    spark.sparkContext.register(sessionAcc)

    // step-5: 根据筛选条件过滤数据，更新累加器。
    val sessionId2FilteredRDD: RDD[(String, String)] = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionAcc)

    // 需要添加一个action算子，触发计算
    sessionId2FilteredRDD.foreach(println)

    // step-6: 统计占比，写入数据库。
    getSessionRatio(spark, taskUUID, sessionAcc.value)

  }

}
