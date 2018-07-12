package com.neusoft.hsyk

import java.util.{Map => JavaMap}

import com.google.gson.{Gson, JsonObject, JsonParser}
import com.neusoft.es.{DateRange, ElasticSearch, IndexDict}
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Response
import com.neusoft.es.Range

import scala.collection.JavaConverters._

/**
  * Created by ZhangQiang on 2018/6/6
  */

object EChartsDataFetcher {


  /**
    * 基本过滤查询
    *
    * @param index        索引名称
    * @param sourceFields 指定要返回的字段
    * @param filters      一切非 range 的过滤条件
    * @param ranges       range 过滤条件
    * @param from         用于分页查询，起始记录
    * @param size         用于分页查询，配合from参数使用，返回多少记录
    * @return
    */
  def smartBasicQuery(index: String,
                      sourceFields: Array[String],
                      filters: JavaMap[String, String],
                      ranges: Array[Range],
                      from: Int,
                      size: Int): String = {
    var buckets = ""

    val response: Response = ElasticSearch.smartSearch(index, sourceFields, filters, ranges, null, null, null, hideSpaceGroup = false, from, size)

    if (response != null) {
      val gson = new Gson()
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val hits = json.getAsJsonObject("hits")
      buckets = gson.toJson(hits)
    }

    println(buckets)

    buckets
  }

  /**
    * 分组查询
    *
    * @param index          索引名称
    * @param sourceFields   指定要返回的字段
    * @param filters        一切非时间的过滤条件
    * @param ranges         时间过滤条件
    * @param groupField     分组字段
    * @param hideSpaceGroup 是否隐藏空白分组，注意对于数值类型的字段，如年龄分组时，必须设置为 false
    * @return
    */
  def smartTermsAgg(index: String,
                    sourceFields: Array[String],
                    filters: JavaMap[String, String],
                    ranges: Array[Range],
                    groupField: String,
                    hideSpaceGroup: Boolean): String = {
    var buckets = ""

    val response: Response = ElasticSearch.smartSearch(index, sourceFields, filters, ranges, groupField, null, null, hideSpaceGroup)

    if (response != null) {
      val gson = new Gson()
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) buckets = gson.toJson(aggregations.getAsJsonObject("nested_agg").getAsJsonObject("group_agg").getAsJsonArray("buckets"))
      else buckets = gson.toJson(aggregations.getAsJsonObject("group_agg").getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }


  /**
    * 日期直方图查询
    *
    * @param index              索引名称
    * @param sourceFields       指定要返回的字段
    * @param filters            一切非时间的过滤条件
    * @param ranges             时间过滤条件
    * @param dateHistogramField 时间直方图字段
    * @param granularity        粒度
    * @return
    */
  def smartDateHistogramAgg(index: String,
                            sourceFields: Array[String],
                            filters: JavaMap[String, String],
                            ranges: Array[Range],
                            dateHistogramField: String,
                            granularity: String): String = {
    var buckets = ""

    val response: Response = ElasticSearch.smartSearch(index, sourceFields, filters, ranges, null, dateHistogramField, granularity, hideSpaceGroup = false)

    if (response != null) {
      val gson = new Gson()
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) buckets = gson.toJson(aggregations.getAsJsonObject("nested_agg").getAsJsonObject("date_histogram_agg").getAsJsonArray("buckets"))
      else buckets = gson.toJson(aggregations.getAsJsonObject("date_histogram_agg").getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }


  /**
    * 分组日期直方图
    *
    * @param index              索引名称
    * @param sourceFields       指定要返回的字段
    * @param filters            一切非时间的过滤条件
    * @param ranges             时间过滤条件
    * @param groupField         分组字段
    * @param dateHistogramField 日期直方图字段
    * @param granularity        粒度
    * @param hideSpaceGroup     隐藏空白分组
    * @return
    */
  def smartTermsDateHistogramAgg(index: String,
                                 sourceFields: Array[String],
                                 filters: JavaMap[String, String],
                                 ranges: Array[Range],
                                 groupField: String,
                                 dateHistogramField: String,
                                 granularity: String,
                                 hideSpaceGroup: Boolean): String = {
    var buckets = ""

    val response: Response = ElasticSearch.smartSearch(index, sourceFields, filters, ranges, groupField, dateHistogramField, granularity, hideSpaceGroup)

    if (response != null) {
      val gson = new Gson()
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) buckets = gson.toJson(aggregations.getAsJsonObject("nested_agg").getAsJsonObject("group_agg").getAsJsonArray("buckets"))
      else buckets = gson.toJson(aggregations.getAsJsonObject("group_agg").getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }

  /**
    * 神奇的搜索
    *
    * 按条件执行查询，需要自己解析返回结果
    *
    * @param index              索引名称
    * @param sourceFields       指定要返回的字段
    * @param filters            一切非时间的过滤条件
    * @param ranges             时间过滤条件
    * @param groupField         分组字段
    * @param dateHistogramField 日期直方图字段
    * @param granularity        粒度
    * @param hideSpaceGroup     隐藏空白分组
    * @param from               用于分页查询，起始记录
    * @param size               用于分页查询，配合from参数使用，返回多少记录
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
                  from: Int,
                  size: Int): String = {

    var responseJson = ""
    val response: Response = ElasticSearch.smartSearch(index, sourceFields, filters, ranges, groupField, dateHistogramField, granularity, hideSpaceGroup, from, size)
    if (response != null) {
      responseJson = EntityUtils.toString(response.getEntity)
    }
    responseJson
  }


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
                 groupField: String): String = {

    var buckets = ""
    val response: Response = ElasticSearch.getMapData(areaFilter, disease, beginDate, endDate, groupField)

    if (response != null) {
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      buckets = new Gson().toJson(json.getAsJsonObject("aggregations")
        .getAsJsonObject("group_agg")
        .getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }


  /**
    * 获取地图数据，需指定粒度
    *
    * 当疾病为null的时候，时间字段为 register_date，即患者的注册时间
    * 当疾病不为null，时间字段为 diseases_standard.first_operate_date，为疾病的第一次诊断时间
    *
    * @param areaFilter  地域过滤条件
    * @param disease     疾病过滤条件
    * @param beginDate   起始筛查日期
    * @param endDate     结束筛查日期
    * @param groupField  分组条件
    * @param granularity 时间分组粒度
    * @return
    */
  def getHistogramMapData(areaFilter: JavaMap[String, String],
                          disease: String,
                          beginDate: String,
                          endDate: String,
                          groupField: String,
                          granularity: String): String = {
    var buckets = ""
    val response: Response = ElasticSearch.getHistogramMapData(areaFilter, disease, beginDate, endDate, groupField, granularity)

    if (response != null) {
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      buckets = new Gson().toJson(json.getAsJsonObject("aggregations")
        .getAsJsonObject("group_agg")
        .getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }


  /**
    * 做为demo演示用，内部逻辑写死，正常应该自己去实现计算逻辑
    *
    * @param filters
    * @param ranges
    * @param distinctField
    * @param groupField
    * @param hideSpaceGroup
    * @return
    */
  def getAvgCost(filters: JavaMap[String, String],
                 ranges: Array[Range],
                 sumCountField: String,
                 distinctField: String,
                 groupField: String,
                 hideSpaceGroup: Boolean = true): String = {

    var buckets = ""

    val sumResponse: Response = ElasticSearch.smartTermsSumCount(IndexDict.customer_all, filters, ranges, sumCountField, groupField, hideSpaceGroup)
    val numResponse: Response = ElasticSearch.smartTermsDistinctCount(IndexDict.customer_all, filters, ranges, distinctField, groupField, hideSpaceGroup)

    if (sumResponse != null && numResponse != null) {

      val sumResponseJson = EntityUtils.toString(sumResponse.getEntity)
      val sumJson: JsonObject = new JsonParser().parse(sumResponseJson).getAsJsonObject
      val areaCost: Array[(String, Double)] = sumJson.getAsJsonObject("aggregations")
        .getAsJsonObject("group_agg")
        .getAsJsonArray("buckets")
        .iterator()
        .asScala
        .map { jsonElem =>
          val jsonObj = jsonElem.getAsJsonObject
          val area = jsonObj.get("key").getAsString
          var sumCost: Double = 0
          if (jsonObj.has("nested_agg")) sumCost = jsonObj.getAsJsonObject("nested_agg").getAsJsonObject("sum_count").get("value").getAsDouble
          else sumCost = jsonObj.getAsJsonObject("sum_count").get("value").getAsDouble
          (area, sumCost)
        }.toArray

      val numResponseJson = EntityUtils.toString(numResponse.getEntity)
      val numJson: JsonObject = new JsonParser().parse(numResponseJson).getAsJsonObject
      val areaNum: Array[(String, Long)] = numJson.getAsJsonObject("aggregations")
        .getAsJsonObject("group_agg")
        .getAsJsonArray("buckets")
        .iterator()
        .asScala
        .map { jsonElem =>
          val jsonObj = jsonElem.getAsJsonObject
          val area = jsonObj.get("key").getAsString
          var num: Long = 0
          if (jsonObj.has("nested_agg")) num = jsonObj.getAsJsonObject("nested_agg").getAsJsonObject("distinct").get("value").getAsLong
          else num = jsonObj.getAsJsonObject("distinct").get("value").getAsLong
          (area, num)
        }.toArray

      val result = (areaCost, areaNum).zipped
        .map { case ((area, sum), (_, num)) =>
          if (num == 0) {
            Map("area" -> area, "avg" -> 0.formatted("%.2f"), "num" -> num).asJava
          } else {
            Map("area" -> area, "avg" -> (sum / num).formatted("%.2f"), "num" -> num).asJava
          }
        }

      buckets = new Gson().toJson(result)
    }

    println(buckets)

    buckets

  }

  /**
    * 根据查询条件获取手术的地区平均消费
    *
    * @param areaFilter
    * @param beginDate
    * @param endDate
    * @param groupField
    * @return
    * @deprecated 应该使用 smartTermsSumCount / smartTermsDistinctCount 自己去计算
    */
  @deprecated
  def getAvgCost(index: String,
                 areaFilter: JavaMap[String, String],
                 beginDate: String,
                 endDate: String,
                 groupField: String): String = {

    var buckets = ""

    val sumResponse: Response = ElasticSearch.getSumCost(index, areaFilter, beginDate, endDate, groupField)
    val numResponse: Response = ElasticSearch.getDistinctNum(index, areaFilter, beginDate, endDate, groupField)

    if (sumResponse != null && numResponse != null) {

      val sumResponseJson = EntityUtils.toString(sumResponse.getEntity)
      val sumJson: JsonObject = new JsonParser().parse(sumResponseJson).getAsJsonObject
      val areaCost: Array[(String, Double)] = sumJson.getAsJsonObject("aggregations")
        .getAsJsonObject("group_agg")
        .getAsJsonArray("buckets")
        .iterator()
        .asScala
        .map { jsonElem =>
          val jsonObj = jsonElem.getAsJsonObject
          val area = jsonObj.get("key").getAsString
          val sumCost = jsonObj.getAsJsonObject("cost").get("value").getAsDouble
          (area, sumCost)
        }.toArray

      val numResponseJson = EntityUtils.toString(numResponse.getEntity)
      val numJson: JsonObject = new JsonParser().parse(numResponseJson).getAsJsonObject
      val areaNum: Array[(String, Int)] = numJson.getAsJsonObject("aggregations")
        .getAsJsonObject("group_agg")
        .getAsJsonArray("buckets")
        .iterator()
        .asScala
        .map { jsonElem =>
          val jsonObj = jsonElem.getAsJsonObject
          val area = jsonObj.get("key").getAsString
          val num = jsonObj.getAsJsonObject("distinct").get("value").getAsInt
          (area, num)
        }.toArray

      val result = (areaCost, areaNum).zipped
        .map { case ((area, sum), (_, num)) => Map("area" -> area, "avg" -> sum / num).asJava }

      buckets = new Gson().toJson(result)
    }

    println(buckets)

    buckets
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
                              hideSpaceGroup: Boolean = true): String = {

    var buckets = ""
    val response: Response = ElasticSearch.smartTermsDistinctCount(index, filters, ranges, distinctField, groupField, hideSpaceGroup)
    if (response != null) {
      val gson = new Gson()
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) buckets = gson.toJson(aggregations.getAsJsonObject("nested_agg").getAsJsonObject("group_agg").getAsJsonArray("buckets"))
      else buckets = gson.toJson(aggregations.getAsJsonObject("group_agg").getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }

  /**
    * 去重计数
    *
    * @param index
    * @param filters
    * @param ranges
    * @param distinctField
    * @return
    */
  def smartDistinctCount(index: String,
                         filters: JavaMap[String, String],
                         ranges: Array[Range],
                         distinctField: String): Long = {
    var results: Long = 0
    val response: Response = ElasticSearch.smartTermsDistinctCount(index, filters, ranges, distinctField, null, hideSpaceGroup = false)
    if (response != null) {
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) results = aggregations.getAsJsonObject("nested_agg").getAsJsonObject("distinct").get("value").getAsLong
      else results = aggregations.getAsJsonObject("distinct").get("value").getAsLong
    }
    println(results)

    results
  }

  /**
    * 分组值累加
    *
    * @param index
    * @param filters
    * @param ranges
    * @param countField
    * @param groupField
    * @param hideSpaceGroup
    * @return
    */
  def smartTermsSumCount(index: String,
                         filters: JavaMap[String, String],
                         ranges: Array[Range],
                         countField: String,
                         groupField: String,
                         hideSpaceGroup: Boolean = true): String = {
    var buckets = ""
    val response: Response = ElasticSearch.smartTermsSumCount(index, filters, ranges, countField, groupField, hideSpaceGroup)
    if (response != null) {
      val gson = new Gson()
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) buckets = gson.toJson(aggregations.getAsJsonObject("nested_agg").getAsJsonObject("group_agg").getAsJsonArray("buckets"))
      else buckets = gson.toJson(aggregations.getAsJsonObject("group_agg").getAsJsonArray("buckets"))
    }

    println(buckets)

    buckets
  }

  /**
    * 值累加
    *
    * @param index
    * @param filters
    * @param ranges
    * @param countField
    * @return
    */
  def smartSumCount(index: String,
                    filters: JavaMap[String, String],
                    ranges: Array[Range],
                    countField: String): Double = {
    var results: Double = 0
    val response: Response = ElasticSearch.smartTermsSumCount(index, filters, ranges, countField, null, hideSpaceGroup = false)
    if (response != null) {
      val responseJson = EntityUtils.toString(response.getEntity)
      val json: JsonObject = new JsonParser().parse(responseJson).getAsJsonObject
      val aggregations = json.getAsJsonObject("aggregations")
      if (aggregations.has("nested_agg")) results = aggregations.getAsJsonObject("nested_agg").getAsJsonObject("sum_count").get("value").getAsDouble
      else results = aggregations.getAsJsonObject("sum_count").get("value").getAsDouble
    }

    println(results)

    results
  }
}
