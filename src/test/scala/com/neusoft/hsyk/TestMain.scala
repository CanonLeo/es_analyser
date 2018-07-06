package com.neusoft.hsyk

import com.neusoft.es.{DateGranularity, DateRange, IndexDict, NumericRange, Range}

object TestMain {
  def main(args: Array[String]): Unit = {
    //    val conditions = Map("province.keyword" -> "辽宁省", "city.keyword" -> "沈阳市,大连市")

    //    println(
    //      EChartsDataFetcher.getMapData(conditions, null, "2011-01-01", "2013-01-01", "province.keyword")
    //    )

    // EChartsDataFetcher.getAvgCost(IndexDict.iol, null, "2011-01-01", "2013-01-01", "province.keyword")

    import scala.collection.JavaConverters._

    //    val filters = Map(
    //      "province.keyword" -> "辽宁省",
    //      "his_diseases_nested.diagnose.keyword" -> "屈光不正",
    //      "his_diseases_nested.age" -> "15"
    //    ).asJava

    val nestedDateRange = new DateRange("diseases_standard.first_operate_date", "2010-01-01", "2013-01-01")
    //val dateRange = new DateRange("register_date", "2010-01-01", "2013-01-01")

    // 相同 path 的 nested timeField 和 nested groupField
    // EChartsDataFetcher.smartTermsDateHistogramAgg(IndexDict.disease_distribution, filters, nestedDateRange, nestedDateRange.field, "diseases_standard.first_age", DateGranularity.YEAR, true)
    // 不同 path 的 nested timeField 和 nested groupField
    // EChartsDataFetcher.smartTermsDateHistogramAgg(IndexDict.disease_distribution, filters, nestedDateRange, nestedDateRange.field, "his_diseases_nested.diagnose.keyword", DateGranularity.YEAR, true)
    // timeField 和 nested groupField
    // EChartsDataFetcher.smartTermsDateHistogramAgg(IndexDict.disease_distribution, filters, dateRange, dateRange.field, "his_diseases_nested.diagnose.keyword", DateGranularity.YEAR, true)
    // nested timeField 和 groupField
    // EChartsDataFetcher.smartTermsDateHistogramAgg(IndexDict.disease_distribution, filters, nestedDateRange, nestedDateRange.field, "city.keyword", DateGranularity.YEAR, true)
    // timeField 和 groupField
    // EChartsDataFetcher.smartTermsDateHistogramAgg(IndexDict.disease_distribution, filters, dateRange, dateRange.field, "city.keyword", DateGranularity.YEAR, true)
    // EChartsDataFetcher.smartBasicQuery(IndexDict.disease_distribution, filters, dateRange)

    // EChartsDataFetcher.smartTermsAgg(IndexDict.disease_distribution, filters, dateRange, "city.keyword", true)
    // EChartsDataFetcher.smartTermsAgg(IndexDict.disease_distribution, filters, dateRange, "diseases_standard.first_age", false)

    // EChartsDataFetcher.smartDateHistogramAgg(IndexDict.disease_distribution, filters, dateRange, nestedDateRange.field, DateGranularity.YEAR)

    //    val conditions = Map("province.keyword" -> "辽宁省").asJava
    //    EChartsDataFetcher.smartTermsDateHistogramAgg(IndexDict.customer_source, conditions, null, "source_name.keyword", "reg_date", DateGranularity.YEAR, true)


    //    EChartsDataFetcher.getMapData(null, null, "", "", "province.keyword")
    //    EChartsDataFetcher.smartBasicQuery(IndexDict.customer_all, null, null, ranges, 0, 100)
    //    EChartsDataFetcher.smartSumCount(IndexDict.customer_all, null, null, "glass_order_info.commodities.cost")
    //    EChartsDataFetcher.smartTermsSumCount(IndexDict.customer_all, null, null, "glass_order_info.commodities.cost", "province.keyword")
    //    EChartsDataFetcher.smartDistinctCount(IndexDict.customer_all, null, null, "glass_order_info.commodities.cost")
    // iol平均消费

    val filters = Map("province.keyword" -> "辽宁省").asJava
    val ranges = Array(new DateRange("operation.iol.fee_date", "2010-01-01", "2015-01-01"), new NumericRange("operation.iol.cost", 300, 10000))

    EChartsDataFetcher.getAvgCost(
      filters,
      ranges,
      sumCountField = "operation.iol.cost",
      distinctField = "card_no.keyword",
      groupField = "city.keyword")
  }
}
