package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnFunctions {

  def bestLowerRemoveAllWhitespace(col: Column): Column = {
    lower(regexp_replace(col, "\\s+", ""))
  }

}
