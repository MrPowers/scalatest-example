package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object NumberHelpers {

  def isEven(inputColName: String, outputColName: String)(df: DataFrame) = {
    df.withColumn(outputColName, col(inputColName) % 2 === lit(0))
  }

def oneThird(inputColName: String, outputColName: String)(df: DataFrame): DataFrame = {
  df.withColumn(outputColName, col(inputColName) / 3)
}

}
