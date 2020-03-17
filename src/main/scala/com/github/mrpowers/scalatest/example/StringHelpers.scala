package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StringHelpers {

  def withStartsWithA(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn("starts_with_a", col(colName).startsWith("a"))
  }

}
