package com.neusoft.es


import java.util.{Collections, Map => JavaMap}

import com.neusoft.hsyk.ElasticSearchUtil
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.{Response, RestClient}

import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConverters._


/**
  * Created by ZhangQiang on 2018/6/6
  */

object ElasticSearch {


  /**
    * 获取地图数据
    *
    * 当疾病为null的时候，时间字段为 register_date，即患者的注册时间
    * 当疾病不为null，时间字段为 diseases_standard.first_operate_date，为疾病的第一次诊断时间
    *
    * @param areaFilter 地域过滤条件
    * @param disease    疾病过滤条件
    * @param beginDate  起始筛查日期
    * @param endDate    结束筛查日期
    * @param groupField 分组条件
    * @return
    */
  def getMapData(areaFilter: JavaMap[String, String],
                 disease: String,
                 beginDate: String,
                 endDate: String,
                 groupField: String): Response = {


    if (disease == null || disease == "") {

      val timeField = "register_date"
      val ranges = new ArrayBuffer[Range]()
      if ((beginDate != null && beginDate != "") || (endDate != null && endDate != ""))
        ranges += new DateRange(timeField, beginDate, endDate)

      smartSearch(IndexDict.disease_distribution, null, areaFilter, ranges.toArray, groupField, null, null)

    } else {

      // 用于存储过滤条件
      val filters = new ArrayBuffer[String]()

      filters += DSL.nestedTermsQuery("diseases_standard", "diseases_standard.diagnose.keyword", disease)

      val timeField = "diseases_standard.first_operate_date"

      if ((beginDate != null && beginDate != "") || (endDate != null && endDate != "")) {
        val dateRange = new DateRange(timeField, beginDate, endDate)
        filters += dateRange.toString
      }

      if (areaFilter != null && areaFilter.size != 0) {
        filters ++= DSL.smartTermsFilters(areaFilter)
      }

      filters += DSL.spaceRegexp(groupField)

      val filterStr = filters.reduce((v1, v2) => s"$v1,$v2")

      val queryDSL: String =
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "filter": [
           |        $filterStr
           |      ]
           |    }
           |  }
           |  ${DSL.termsAgg(groupField)}
           |}
         """.stripMargin
      println(queryDSL)
      executeSearch(IndexDict.disease_distribution, queryDSL)
    }
  }

  /**
    *
    * @param areaFilter
    * @param disease
    * @param beginDate
    * @param endDate
    * @param groupField
    * @param granularity
    * @return
    */
  def getHistogramMapData(areaFilter: JavaMap[String, String],
                          disease: String,
                          beginDate: String,
                          endDate: String,
                          groupField: String,
                          granularity: String): Response = {

    if (disease == null || disease == "") {

      val timeField = "register_date"
      val ranges = new ArrayBuffer[Range]()
      if (beginDate != null || endDate != null) ranges += new DateRange(timeField, beginDate, endDate)
      smartSearch(IndexDict.disease_distribution, null, areaFilter, ranges.toArray, groupField, timeField, granularity)

    } else {

      // 用于存储过滤条件
      val filters = new ArrayBuffer[String]()

      // 添加疾病过滤条件
      filters += DSL.nestedTermsQuery("diseases_standard", "diseases_standard.diagnose.keyword", disease)

      // 定义时间字段
      val timeField = "diseases_standard.first_operate_date"
      if ((beginDate != null && beginDate != "") || (endDate != null && endDate != "")) {
        val dateRange = new DateRange(timeField, beginDate, endDate)
        filters += dateRange.toString
      }

      if (areaFilter != null && areaFilter.size != 0) {
        filters ++= DSL.smartTermsFilters(areaFilter)
      }

      filters += DSL.spaceRegexp(groupField)

      val filterStr = filters.reduce((v1, v2) => s"$v1,$v2")

      val queryDSL: String =
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "filter": [
           |        $filterStr
           |      ]
           |    }
           |  },
           |  "aggs": {
           |    "group_agg": {
           |      "terms": {
           |        "field": "$groupField",
           |        "size": ${Int.MaxValue}
           |      }
           |      ${DSL.nestedDateHistogramAgg("diseases_standard", timeField, granularity)}
           |    }
           |  }
           |}
             """.stripMargin

      println(queryDSL)
      executeSearch(IndexDict.disease_distribution, queryDSL)
    }
  }


  /**
    * 神奇的搜索
    *
    * @param index              索引名称
    * @param filters            一切非时间的过滤条件 @example
    *                           select * from $index where PROVINCE = "辽宁省" and CITY in ("沈阳", "大连")
    *                           conditions = [("PROVINCE", "辽宁省"), ("CITY", "沈阳,大连")]
    * @param ranges             ranges过滤条件
    * @param groupField         分组字段
    * @param dateHistogramField 日期直方图字段
    * @param granularity        粒度
    * @param hideSpaceGroup     是否隐藏空白分组
    * @return
    */
  def smartSearch(index: String,
                  sourceFields: Array[String],
                  filters: JavaMap[String, String],
                  ranges: Array[Range],
                  groupField: String,
                  dateHistogramField: String,
                  granularity: String,
                  hideSpaceGroup: Boolean = true,
                  from: Int = 0,
                  size: Int = 1): Response = {

    /* 指定需要返回的字段信息 */
    var _source = ""
    if (sourceFields != null) _source = sourceFields.map(field =>s""""$field"""").mkString(",")

    /* =========== 构造过滤条件 =========== */

    val filterConditions = new ArrayBuffer[String]()

    if (filters != null && !filters.isEmpty) filterConditions ++= DSL.smartTermsFilters(filters)

    if (ranges != null) filterConditions ++= ranges.map(_.toString)

    if (hideSpaceGroup) filterConditions += DSL.spaceRegexp(groupField)

    val filterStr = filterConditions.mkString(",")


    /* =========== 构造聚合条件 =========== */

    var aggStr = ""

    if (groupField == null || groupField == "") {
      if (dateHistogramField != null && dateHistogramField != "")
        aggStr = DSL.smartDateHistogramAgg(dateHistogramField, granularity)
    }
    else {
      if (dateHistogramField != null && dateHistogramField != "")
        aggStr = DSL.smartTermsDateHistogramAgg(groupField, dateHistogramField, granularity)
      else
        aggStr = DSL.smartTermsAgg(groupField)
    }

    /* =========== 生成查询语句 =========== */

    val queryDSL =
      s"""
         |{
         |    "query": {
         |        "bool": {
         |            "filter": [
         |              $filterStr
         |            ]
         |        }
         |    },
         |    "from": $from,
         |    "size": $size,
         |    "_source": [ ${_source} ]
         |    $aggStr
         |}
             """.stripMargin

    println(queryDSL)

    executeSearch(index, queryDSL)
  }

  /**
    * 按字段计数
    *
    * @param index
    * @param filters
    * @param ranges
    * @param countField
    * @param groupField
    * @return
    */
  def smartTermsSumCount(index: String,
                         filters: JavaMap[String, String],
                         ranges: Array[Range],
                         countField: String,
                         groupField: String,
                         hideSpaceGroup: Boolean = true): Response = {

    /* ========== 构造过滤条件 ========== */
    val filterConditions = new ArrayBuffer[String]()

    if (filters != null && !filters.isEmpty) filterConditions ++= DSL.smartTermsFilters(filters)

    if (ranges != null) filterConditions ++= ranges.map(_.toString)

    if (hideSpaceGroup) filterConditions += DSL.spaceRegexp(groupField)

    val filterStr = filterConditions.mkString(",")

    /* =========== 构造聚合条件 =========== */
    var aggStr = ""
    if (groupField == null || groupField == "") aggStr = DSL.smartSumCount(countField)
    else aggStr = DSL.smartTermsSumCount(countField, groupField)

    val queryDSL: String =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": [
         |        $filterStr
         |      ]
         |    }
         |  },
         |  $aggStr
         |}
         """.stripMargin

    println(queryDSL)

    executeSearch(index, queryDSL)

  }

  /**
    * 根据查询条件获取准分子手术的总消费
    *
    * @param areaFilter
    * @param beginDate
    * @param endDate
    * @param groupField
    * @deprecated 使用 smartSumCount 或者 smartTermsSumCount
    * @return
    */
  @deprecated
  def getSumCost(index: String,
                 areaFilter: JavaMap[String, String],
                 beginDate: String,
                 endDate: String,
                 groupField: String): Response = {

    val costField = index match {
      case IndexDict.excimer => "own_cost"
      case IndexDict.iol => "tot_cost"
      case _ =>
    }

    // 用于存储过滤条件
    val filters = new ArrayBuffer[String]()

    val timeField = "fee_date"
    if ((beginDate != null && beginDate != "") || (endDate != null && endDate != "")) {
      val dateRange = new DateRange(timeField, beginDate, endDate)
      filters += dateRange.toString
    }

    if (areaFilter != null && areaFilter.size != 0) {
      filters ++= DSL.smartTermsFilters(areaFilter)
    }

    filters += DSL.spaceRegexp(groupField)

    val filterStr = filters.reduce((v1, v2) => s"$v1,$v2")

    val queryDSL: String =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": [
         |        $filterStr
         |      ]
         |    }
         |  },
         |  "aggs": {
         |    "group_agg": {
         |      "terms": {
         |        "field": "$groupField",
         |        "size": ${Int.MaxValue}
         |      },
         |      "aggs" : {
         |        "cost" : { "sum" : { "field" : "$costField" } }
         |      }
         |    }
         |  }
         |}
         """.stripMargin

    println(queryDSL)

    executeSearch(index, queryDSL)
  }

  /**
    * 分组去重计数
    *
    * @param index
    * @param filters
    * @param ranges
    * @param distinctField
    * @param groupField
    * @param hideSpaceGroup
    * @return
    */
  def smartTermsDistinctCount(index: String,
                              filters: JavaMap[String, String],
                              ranges: Array[Range],
                              distinctField: String,
                              groupField: String,
                              hideSpaceGroup: Boolean = true) = {
    /* ========== 构造过滤条件 ========== */
    val filterConditions = new ArrayBuffer[String]()

    if (filters != null && !filters.isEmpty) filterConditions ++= DSL.smartTermsFilters(filters)

    if (ranges != null) filterConditions ++= ranges.map(_.toString)

    if (hideSpaceGroup) filterConditions += DSL.spaceRegexp(groupField)

    val filterStr = filterConditions.mkString(",")

    /* =========== 构造聚合条件 =========== */
    var aggStr = ""
    if (groupField == null || groupField == "") aggStr = DSL.smartDistinctCount(distinctField)
    else aggStr = DSL.smartTermsDistinctCount(distinctField, groupField)

    val queryDSL: String =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": [
         |        $filterStr
         |      ]
         |    }
         |  },
         |  $aggStr
         |}
         """.stripMargin

    println(queryDSL)

    executeSearch(index, queryDSL)
  }


  /**
    * 根据查询条件获取准分子手术人数
    *
    * @param areaFilter
    * @param beginDate
    * @param endDate
    * @param groupField
    * @deprecated 使用 smartDistinctCount 或者 smartTermsDistinctCount
    * @return
    */
  @deprecated
  def getDistinctNum(index: String,
                     areaFilter: JavaMap[String, String],
                     beginDate: String,
                     endDate: String,
                     groupField: String): Response = {
    // 用于存储过滤条件
    val filters = new ArrayBuffer[String]()

    val timeField = "fee_date"
    if ((beginDate != null && beginDate != "") || (endDate != null && endDate != "")) {
      val dateRange = new DateRange(timeField, beginDate, endDate)
      filters += dateRange.toString
    }

    if (areaFilter != null && areaFilter.size != 0) {
      filters ++= DSL.smartTermsFilters(areaFilter)
    }

    filters += DSL.spaceRegexp(groupField)

    val filterStr = filters.reduce((v1, v2) => s"$v1,$v2")

    val queryDSL: String =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": [
         |        $filterStr
         |      ]
         |    }
         |  },
         |  "aggs": {
         |    "group_agg": {
         |      "terms": {
         |        "field": "$groupField",
         |        "size": ${Int.MaxValue}
         |      },
         |      "aggs": {
         |        "distinct": {
         |          "cardinality": {
         |            "field": "card_no.keyword",
         |            "precision_threshold": 40000
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
         """.stripMargin

    println(queryDSL)

    executeSearch(index, queryDSL)
  }


  /**
    * Synchronized request
    *
    * @param index
    * @param queryDSL
    * @return
    */
  def executeSearch(index: String, queryDSL: String): Response = {

    val params = Collections.emptyMap[String, String]()

    val entity: HttpEntity = new NStringEntity(queryDSL, ContentType.APPLICATION_JSON)
    val restClient: RestClient = ElasticSearchUtil.getRestClientBuilder.build
    var response: Response = null
    try {
      response = restClient.performRequest("GET", s"/$index/_search", params, entity)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      restClient.close()
    }
    response
  }

}
