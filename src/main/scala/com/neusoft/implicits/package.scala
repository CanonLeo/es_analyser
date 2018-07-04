package com.neusoft

import com.neusoft.es.Field

/**
  * Created by ZhangQiang 2018/5/8 15:29
  */
package object implicits {

  implicit def string2Field(name: String): Field = new Field(name)

}
