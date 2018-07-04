package com.neusoft.es

import scala.collection.mutable.ArrayBuffer
import com.neusoft.implicits.string2Field

class NumericRange(field: String, var begin: Double = 0, var end: Double = 0) extends Range {

  override def toString: String = {

    var rangeExp = ""

    if (!(begin == 0 && end == 0)) {

      if (field == null || field.name == "") throw new IllegalArgumentException("Range.field 不能为 null 或空串!")

      val conditions = new ArrayBuffer[String]()
      if (begin != 0) conditions += s""""gte": "$begin""""
      if (end != 0) conditions += s""""lte": "$end""""

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
             |                    ${conditions.mkString(",")}
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
             |                ${conditions.mkString(",")}
             |            }
             |          }
             |        }
       """.stripMargin

      }
    }

    rangeExp
  }
}
