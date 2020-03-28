package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Aggregations {

  def totalGoalsAgg()(df: DataFrame): DataFrame = {
    df
      .groupBy("name")
      .agg(sum("goals").as("total_goals"))
  }

  def studentsAgg()(df: DataFrame): DataFrame = {
    df
      .groupBy("continent", "country")
      .count()
  }

  def averageGoalsAgg()(df: DataFrame): DataFrame  ={
    df
      .groupBy("name")
      .agg(avg("goals"), avg("assists").as("average_assists"))
      .where(col("average_assists") >= 100)
  }

}
