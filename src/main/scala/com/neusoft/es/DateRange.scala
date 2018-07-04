package com.neusoft.es

import com.neusoft.implicits.string2Field

/**
  * Created by Zhangqiang on 2018/6/6
  */

class DateRange(var field: String, var begin: String, var end: String) extends Range {


  override def toString: String = {

    var rangeExp = ""

    if (!(begin == null && end == null)) {

      if (field == null || field.name == "") throw new IllegalArgumentException("DateRange.field 不能为 null 或空串!")

      var gte = ""
      var lte = ""
      if (begin != null) gte = s""""gte": "$begin","""
      if (end != null) lte = s""""lte": "$end","""

      if (field.isNested) {

        val path = field.nestedPath
        rangeExp =
          s"""
             |        {
             |          "nested": {
             |            "path": "$path",
             |            "query": {
             |              "range": {
             |                "$field": {
             |                    $gte
             |                    $lte
             |                    "format": "yyyy-MM-dd||yyyy||yyyy/MM/dd"
             |                }
             |              }
             |            }
             |          }
             |        }
       """.stripMargin
      } else {

        rangeExp =
          s"""
             |        {
             |          "range": {
             |            "$field": {
             |                $gte
             |                $lte
             |                "format": "yyyy-MM-dd||yyyy||yyyy/MM/dd"
             |            }
             |          }
             |        }
       """.stripMargin

      }
    }

    rangeExp
  }


}
