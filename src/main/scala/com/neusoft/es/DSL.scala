package com.neusoft.es

import java.util.{Map => JavaMap}
// implicits
import scala.collection.JavaConversions._
import com.neusoft.implicits.string2Field

/**
  * Created by ZhangQiang on 2018/6/6
  */

protected object DSL {

  /**
    * 去重计数
    *
    * @param distinctField
    * @return
    */
  def smartDistinctCount(distinctField: String): String = {
    if (distinctField.isNested) {
      s"""
         |      "aggs": {
         |        "nested_agg": {
         |            "nested": {
         |                "path": "${distinctField.nestedPath}"
         |            },
         |            "aggs": {
         |              "distinct": {
         |                "cardinality": {
         |                  "field": "$distinctField",
         |                  "precision_threshold": 40000
         |                }
         |              }
         |            }
         |        }
         |    }
       """.stripMargin
    } else {
      s"""
         |            "aggs": {
         |              "distinct": {
         |                "cardinality": {
         |                  "field": "$distinctField",
         |                  "precision_threshold": 40000
         |                }
         |              }
         |            }
       """.stripMargin
    }
  }

  /**
    * 分组去重计数
    *
    * @param distinctField
    * @param groupField
    * @return
    */
  def smartTermsDistinctCount(distinctField: String, groupField: String): String = {
    if (distinctField.isNested && groupField.isNested) {

      val groupPath = groupField.nestedPath
      val sumCountPath = distinctField.nestedPath

      if (groupPath == sumCountPath) {

        s"""
           |      "aggs": {
           |        "nested_agg": {
           |            "nested": {
           |                "path": "${groupField.nestedPath}"
           |            },
           |            "aggs": {
           |                "group_agg": {
           |                    "terms": {
           |                        "field": "$groupField",
           |                        "size": ${Int.MaxValue}
           |                    },
           |                    "aggs": {
           |                       "distinct": {
           |                           "cardinality": {
           |                               "field": "$distinctField",
           |                               "precision_threshold": 40000
           |                           }
           |                        }
           |                    }
           |                }
           |            }
           |        }
           |    }
         """.stripMargin

      } else {

        s"""
           |    "aggs": {
           |        "nested_agg": {
           |            "nested": {
           |                "path": "$groupPath"
           |            },
           |            "aggs": {
           |                "group_agg": {
           |                    "terms": {
           |                        "field": "$groupField",
           |                        "size": ${Int.MaxValue}
           |                    },
           |                    "aggs": {
           |                        "reverse_nested_agg": {
           |                            "reverse_nested": {},
           |                            "aggs": {
           |                                "nested_agg": {
           |                                    "nested": {
           |                                        "path": "$sumCountPath"
           |                                    },
           |                                    "aggs": {
           |                                      "distinct": {
           |                                          "cardinality": {
           |                                              "field": "$distinctField",
           |                                               "precision_threshold": 40000
           |                                           }
           |                                        }
           |                                    }
           |                                }
           |                            }
           |                        }
           |                    }
           |                }
           |            }
           |        }
           |    }
       """.stripMargin
      }
    } else if (!distinctField.isNested && groupField.isNested) {
      s"""
         |      "aggs": {
         |        "nested_agg": {
         |            "nested": {
         |                "path": "${groupField.nestedPath}"
         |            },
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs": {
         |                        "reverse_nested": {
         |                            "reverse_nested": {},
         |                            "aggs": {
         |                              "distinct": {
         |                                  "cardinality": {
         |                                      "field": "$distinctField",
         |                                      "precision_threshold": 40000
         |                                   }
         |                                }
         |                            }
         |                        }
         |                    }
         |                }
         |            }
         |        }
         |    }
         """.stripMargin
    } else if (distinctField.isNested && !groupField.isNested) {
      s"""
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs": {
         |                        "nested_agg": {
         |                            "nested": {
         |                                "path": "${distinctField.nestedPath}"
         |                            },
         |                            "aggs": {
         |                              "distinct": {
         |                                  "cardinality": {
         |                                      "field": "$distinctField",
         |                                      "precision_threshold": 40000
         |                                   }
         |                                }
         |                            }
         |                        }
         |                    }
         |                }
         |            }
        """.stripMargin
    } else {
      s"""
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs": {
         |                        "distinct": {
         |                            "cardinality": {
         |                               "field": "$distinctField",
         |                               "precision_threshold": 40000
         |                            }
         |                        }
         |                    }
         |                }
         |            }
        """.stripMargin
    }
  }

  /**
    * 分组计算字段的和
    *
    * @param sumCountField
    * @param groupField
    * @return
    */
  def smartTermsSumCount(sumCountField: String, groupField: String) = {
    if (sumCountField.isNested && groupField.isNested) {

      val groupPath = groupField.nestedPath
      val sumCountPath = sumCountField.nestedPath

      if (groupPath == sumCountPath) {

        s"""
           |      "aggs": {
           |        "nested_agg": {
           |            "nested": {
           |                "path": "${groupField.nestedPath}"
           |            },
           |            "aggs": {
           |                "group_agg": {
           |                    "terms": {
           |                        "field": "$groupField",
           |                        "size": ${Int.MaxValue}
           |                    },
           |                    "aggs" : {
           |                        "sum_count" : { "sum" : { "field" : "$sumCountField" } }
           |                    }
           |                }
           |            }
           |        }
           |    }
         """.stripMargin

      } else {

        s"""
           |    "aggs": {
           |        "nested_agg": {
           |            "nested": {
           |                "path": "$groupPath"
           |            },
           |            "aggs": {
           |                "group_agg": {
           |                    "terms": {
           |                        "field": "$groupField",
           |                        "size": ${Int.MaxValue}
           |                    },
           |                    "aggs": {
           |                        "reverse_nested_agg": {
           |                            "reverse_nested": {},
           |                            "aggs": {
           |                                "nested_agg": {
           |                                    "nested": {
           |                                        "path": "$sumCountPath"
           |                                    },
           |                                    "aggs" : {
           |                                      "sum_count" : { "sum" : { "field" : "$sumCountField" } }
           |                                    }
           |                                }
           |                            }
           |                        }
           |                    }
           |                }
           |            }
           |        }
           |    }
       """.stripMargin
      }
    } else if (!sumCountField.isNested && groupField.isNested) {
      s"""
         |      "aggs": {
         |        "nested_agg": {
         |            "nested": {
         |                "path": "${groupField.nestedPath}"
         |            },
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs": {
         |                        "reverse_nested": {
         |                            "reverse_nested": {},
         |                            "aggs" : {
         |                              "sum_count" : { "sum" : { "field" : "$sumCountField" } }
         |                            }
         |                        }
         |                    }
         |                }
         |            }
         |        }
         |    }
         """.stripMargin
    } else if (sumCountField.isNested && !groupField.isNested) {
      s"""
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs": {
         |                        "nested_agg": {
         |                            "nested": {
         |                                "path": "${sumCountField.nestedPath}"
         |                            },
         |                            "aggs" : {
         |                                "sum_count" : { "sum" : { "field" : "$sumCountField" } }
         |                            }
         |                        }
         |                    }
         |                }
         |            }
        """.stripMargin
    } else {
      s"""
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs" : {
         |                        "sum_count" : { "sum" : { "field" : "$sumCountField" } }
         |                    }
         |                }
         |            }
        """.stripMargin
    }
  }


  def smartSumCount(sumCountField: String): String = {
    if (sumCountField.isNested) {
      s"""
         |      "aggs": {
         |        "nested_agg": {
         |            "nested": {
         |                "path": "${sumCountField.nestedPath}"
         |            },
         |            "aggs" : {
         |                "sum_count" : { "sum" : { "field" : "$sumCountField" } }
         |            }
         |        }
         |    }
       """.stripMargin
    } else {
      s"""
         |      "aggs" : {
         |        "sum_count" : { "sum" : { "field" : "$sumCountField" } }
         |      }
       """.stripMargin
    }
  }

  def smartTermsAgg(groupField: String, isAppend: Boolean = true): String = {

    var symbol = ""
    if (isAppend) symbol = ","


    if (groupField.isNested) {

      s"""
         |      $symbol
         |      "aggs": {
         |        "nested_agg": {
         |            "nested": {
         |                "path": "${groupField.nestedPath}"
         |            },
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    }
         |                }
         |            }
         |        }
         |    }
       """.stripMargin

    } else {

      s"""
         |      $symbol
         |      "aggs": {
         |        "group_agg": {
         |          "terms": {
         |             "field": "$groupField",
         |             "size": ${Int.MaxValue}
         |          }
         |        }
         |     }
       """.stripMargin

    }
  }

  /**
    * 自动判断字段类型(nested/non-nested)，生成合法的 DateHistogram DSL 语句
    *
    * @param dateHistogramField
    * @param granularity
    * @param isAppend
    * @return
    */
  def smartDateHistogramAgg(dateHistogramField: String,
                            granularity: String = DateGranularity.MONTH,
                            isAppend: Boolean = true): String = {
    var symbol = ""
    if (isAppend) symbol = ","

    if (dateHistogramField.isNested) {
      s"""
         |      $symbol
         |      "aggs": {
         |          "nested_agg": {
         |              "nested": {
         |                  "path": "${dateHistogramField.nestedPath}"
         |              },
         |              "aggs": {
         |                 "date_histogram_agg": {
         |                     "date_histogram": {
         |                         "field": "$dateHistogramField",
         |                         "interval": "$granularity",
         |                         "format": "yyyy-MM-dd"
         |                      }
         |                 }
         |             }
         |         }
         |     }
     """.stripMargin
    }
    else {
      s"""
         |  $symbol
         |  "aggs": {
         |    "date_histogram_agg": {
         |      "date_histogram": {
         |        "field": "$dateHistogramField",
         |        "interval": "$granularity",
         |        "format": "yyyy-MM-dd"
         |      }
         |    }
         |  }
     """.stripMargin
    }
  }

  /**
    * 自动判断字段类型(nested/non-nested)，生成合法的 Terms DateHistogram DSL 语句
    *
    * @param groupField  分组字段
    * @param dateField   histogram 字段
    * @param granularity 时间粒度
    * @param isAppend    是否在开始添加英文逗号
    * @return
    */
  def smartTermsDateHistogramAgg(groupField: String,
                                 dateField: String,
                                 granularity: String = DateGranularity.MONTH,
                                 isAppend: Boolean = true): String = {
    var symbol = ""
    if (isAppend) symbol = ","

    if (!groupField.isNested && dateField.isNested) {

      s"""
         |      $symbol
         |      "aggs": {
         |        "group_agg": {
         |          "terms": {
         |            "field": "$groupField",
         |            "size": ${Int.MaxValue}
         |          },
         |          "aggs": {
         |            "nested_agg": {
         |              "nested": {
         |                "path": "${dateField.nestedPath}"
         |              },
         |              "aggs": {
         |                "date_histogram_agg": {
         |                  "date_histogram": {
         |                    "field": "$dateField",
         |                    "interval": "$granularity",
         |                    "format": "yyyy-MM-dd"
         |                  }
         |                }
         |              }
         |            }
         |          }
         |        }
         |      }
     """.stripMargin

    } else if (groupField.isNested && dateField.isNested) {

      val groupPath = groupField.nestedPath
      val histogramPath = dateField.nestedPath

      if (groupPath == histogramPath) {

        s"""
           |      $symbol
           |      "aggs": {
           |        "nested_agg": {
           |            "nested": {
           |                "path": "$groupPath"
           |            },
           |            "aggs": {
           |                "group_agg": {
           |                    "terms": {
           |                        "field": "$groupField",
           |                        "size": ${Int.MaxValue}
           |                    },
           |                    "aggs": {
           |                        "date_histogram_agg": {
           |                            "date_histogram": {
           |                                "field": "$dateField",
           |                                "interval": "$granularity",
           |                                "format": "yyyy-MM-dd"
           |                            }
           |                        }
           |                    }
           |                }
           |            }
           |        }
           |    }
       """.stripMargin

      } else {

        s"""
           |    $symbol
           |    "aggs": {
           |        "nested_agg": {
           |            "nested": {
           |                "path": "$groupPath"
           |            },
           |            "aggs": {
           |                "group_agg": {
           |                    "terms": {
           |                        "field": "$groupField",
           |                        "size": ${Int.MaxValue}
           |                    },
           |                    "aggs": {
           |                        "reverse_nested_agg": {
           |                            "reverse_nested": {},
           |                            "aggs": {
           |                                "nested_agg": {
           |                                    "nested": {
           |                                        "path": "$histogramPath"
           |                                    },
           |                                    "aggs": {
           |                                        "date_histogram_agg": {
           |                                            "date_histogram": {
           |                                                "field": "$dateField",
           |                                                "interval": "$granularity",
           |                                                "format": "yyyy-MM-dd"
           |                                            }
           |                                        }
           |                                    }
           |                                }
           |                            }
           |                        }
           |                    }
           |                }
           |            }
           |        }
           |    }
       """.stripMargin
      }

    } else if (groupField.isNested && !dateField.isNested) {

      s"""
         |    $symbol
         |    "aggs": {
         |        "nested_agg": {
         |            "nested": {
         |                "path": "${groupField.nestedPath}"
         |            },
         |            "aggs": {
         |                "group_agg": {
         |                    "terms": {
         |                        "field": "$groupField",
         |                        "size": ${Int.MaxValue}
         |                    },
         |                    "aggs": {
         |                        "reverse_nested": {
         |                            "reverse_nested": {},
         |                            "aggs": {
         |                                "date_histogram_agg": {
         |                                    "date_histogram": {
         |                                        "field": "$dateField",
         |                                        "interval": "$granularity",
         |                                        "format": "yyyy-MM-dd"
         |                                    }
         |                                }
         |                            }
         |                        }
         |                    }
         |                }
         |            }
         |        }
         |    }
       """.stripMargin

    } else {
      terms_dateHistogramAgg(groupField, dateField, granularity, isAppend)
    }

  }

  def spaceRegexp(field: String): String = {
    s"""
       |        {
       |          "bool":{
       |            "must_not":{
       |              "regexp":{
       |                "$field": {
       |                  "value": "\\\\s*",
       |                  "flags": "ALL"
       |                }
       |              }
       |            }
       |          }
       |        }
       """.stripMargin
  }

  /**
    * 将 Map 中的每个条件转换成合法的 DSL 的过滤语句
    *
    * @param filterConditionMap
    * @return 合法的过滤语句组成的数组
    */
  def smartTermsFilters(filterConditionMap: JavaMap[String, String]): Array[String] = {

    val nestedConditions = filterConditionMap
      .filter { case (key, _) => key.isNested }
      .groupBy { case (key, _) => key.nestedPath }
      .map { case (path, conditions) =>

        val nestedStr = conditions
          .map { case (field, values) =>
            val matchTerms = values.split(",").map(v => s""""${v.replaceAll(" ", "")}"""").reduce((v1, v2) => s"""$v1, $v2""")
            s"""
               |                  {
               |                    "terms": {"$field": [ $matchTerms ]}
               |                  }
              """.stripMargin
          }
          .mkString(",")

        s"""
           |        {
           |          "nested": {
           |            "path": "$path",
           |            "query": {
           |              "bool": {
           |                "must": [
           |                  $nestedStr
           |                ]
           |              }
           |            }
           |          }
           |        }
          """.stripMargin
      }
      .toArray

    val ordConditions = filterConditionMap
      .filterNot { case (key, _) => key.isNested }
      .map { case (field, value) => termsQuery(field, value) }
      .toArray

    nestedConditions ++: ordConditions
  }

  /**
    * 将合法的过滤语句数组转换成字符串
    *
    * @param filterConditionMap
    * @return
    */
  def smartTermsFilterStr(filterConditionMap: JavaMap[String, String]): String = smartTermsFilters(filterConditionMap).reduce((v1, v2) => s"$v1,$v2")


  /**
    *
    * @param field
    * @param values
    * @return
    */
  def termsQuery(field: String, values: String): String = {
    val matchTerms = values.split(",").map(v => s""""${v.replaceAll(" ", "")}"""").reduce((v1, v2) => s"""$v1, $v2""")
    s"""
       |        {
       |          "terms": {
       |            "$field": [ $matchTerms ]
       |          }
       |        }
         """.stripMargin
  }

  def nestedTermsQuery(path: String,
                       field: String,
                       values: String): String = {

    val matchTerms = values.split(",").map(v => s""""${v.replaceAll(" ", "")}"""").reduce((v1, v2) => s"""$v1, $v2""")

    s"""
       |        {
       |          "nested": {
       |            "path": "$path",
       |            "query": {
       |              "terms": {
       |                "$field": [ $matchTerms ]
       |              }
       |            }
       |          }
       |        }
     """.stripMargin
  }


  def termsAgg(groupField: String,
               isAppend: Boolean = true): String = {

    var symbol = ""
    if (isAppend) symbol = ","

    s"""
       |  $symbol
       |  "aggs": {
       |    "group_agg": {
       |      "terms": {
       |        "field": "$groupField",
       |        "size": ${Int.MaxValue}
       |      }
       |    }
       |  }
       """.stripMargin
  }


  def dateHistogramAgg(dateField: String,
                       granularity: String = DateGranularity.MONTH,
                       isAppend: Boolean = true): String = {

    var symbol = ""
    if (isAppend) symbol = ","

    s"""
       |  $symbol
       |  "aggs": {
       |    "date_histogram_agg": {
       |      "date_histogram": {
       |        "field": "$dateField",
       |        "interval": "$granularity",
       |        "format": "yyyy-MM-dd"
       |      }
       |    }
       |  }
     """.stripMargin
  }

  def terms_dateHistogramAgg(groupField: String,
                             dateField: String,
                             granularity: String = DateGranularity.MONTH,
                             isAppend: Boolean = true): String = {

    var symbol = ""
    if (isAppend) symbol = ","

    s"""
       |  $symbol
       |  "aggs": {
       |    "group_agg": {
       |      "terms": {
       |        "field": "$groupField",
       |        "size": ${Int.MaxValue}
       |      }
       |      ${dateHistogramAgg(dateField, granularity)}
       |    }
       |  }
     """.stripMargin
  }


  def nestedDateHistogramAgg(path: String, dateField: String,
                             granularity: String = DateGranularity.MONTH,
                             isAppend: Boolean = true): String = {

    var symbol = ""
    if (isAppend) symbol = ","

    s"""
       |      $symbol
       |      "aggs": {
       |        "nested_agg": {
       |          "nested": {
       |            "path": "$path"
       |          }
       |          ${dateHistogramAgg(dateField, granularity)}
       |        }
       |      }
     """.stripMargin
  }


}
