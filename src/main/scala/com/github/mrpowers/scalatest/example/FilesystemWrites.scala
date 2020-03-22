package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

object FilesystemWrites extends SparkSessionWrapper {

  def withIsOdd(
     inputColName: String,
     outputColName: String = "is_odd"
   )(df: DataFrame): DataFrame = {
    df.withColumn(outputColName, col(inputColName) % 2 === lit(1))
  }

  def writesHockeyPlayers(outputPath: String): Unit = {
    val df = spark.createDF(
      List(
        ("leech", 2),
        ("hull", 16),
        ("crosby", 87)
      ), List(
        ("last_name", StringType, true),
        ("jersey_number", IntegerType, true)
      )
    ).transform(withIsOdd("jersey_number", "is_jersey_odd"))

    df.repartition(1).write.parquet(outputPath)
  }

}
