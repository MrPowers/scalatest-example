package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object OrganizingTests {

  def analyzeTextFeeling(text: String): String = {
    if (text contains "happy" ) "positive" else "negative"
  }

  def withAnalyzedFeeling(inputColName: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      "analyzed_feeling",
      when(col(inputColName).isNull, null)
        .when(col(inputColName).contains("happy"), "positive")
        .otherwise("negative")
    )
  }

}
