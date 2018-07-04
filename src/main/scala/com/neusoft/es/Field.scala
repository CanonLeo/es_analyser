package com.neusoft.es

/**
  * ElasticSearch 字段
  *
  * @param name
  */
class Field(var name: String) {


  // 所有 nested 字段
  val nestedFields = Set(
    "diseases_standard",
    "his_diseases_nested",
    "register_his",
    "operation.iol",
    "operation.excimer")

  lazy val prefix = getPrefix

  def isNested: Boolean = nestedFields.contains(prefix)

  def nestedPath: String = {
    if (isNested) prefix else ""
  }

  /**
    * 字段的前缀名称，例如
    * diseases_standard.diagnose 的前缀名称为 diseases_standard
    * operation.iol.fee_date 的前缀名称为 operation.iol
    */
  def getPrefix: String = {
    val names = name.split("\\.")
    names.zipWithIndex
      .filter { case (_, index) =>
        val size = names.length
        if (size == 1) true
        else index < size - 1
      }
      .map { case (name, _) => name }
      .mkString(".")
  }

  override def toString: String = name
}
