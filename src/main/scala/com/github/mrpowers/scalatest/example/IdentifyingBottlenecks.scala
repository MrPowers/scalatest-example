package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object IdentifyingBottlenecks {

  def withRunEverything()(df: DataFrame): DataFrame = {
    df
      .transform(withCol1())
      .transform(withCol2())
      .transform(withCol3())
  }

  def withCol1()(df: DataFrame): DataFrame = {
    df.withColumn("col1", lit("c1"))
  }

  def withCol2()(df: DataFrame): DataFrame = {
    Thread.sleep(8000)
    df.withColumn("col2", lit("c2"))
  }

  def withCol3()(df: DataFrame): DataFrame = {
    df.withColumn("col3", lit("c3"))
  }

}
