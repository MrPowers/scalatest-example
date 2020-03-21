package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.functions._

object UserDefinedFunctions {

  def lowerRemoveAllWhitespaceFun(s: String): String = {
    s.toLowerCase().replaceAll("\\s", "")
  }

  val lowerRemoveAllWhitespace = udf[String, String](lowerRemoveAllWhitespaceFun)

}
