package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object DateFunctions {

  def withTwoDaysEarlier(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      "two_days_earlier",
      date_add(col(colName), -2)
    )
  }

}
