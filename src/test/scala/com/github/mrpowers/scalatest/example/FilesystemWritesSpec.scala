package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import FilesystemWrites._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import com.github.mrpowers.spark.daria.utils.NioUtils

class FilesystemWritesSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer
    with DataFrameComparer {

  describe("isOdd") {
    it("returns true if the number is odd") {
      val df = spark.createDF(
        List(
          (1, true),
          (8, false),
          (null, null)
        ), List(
          ("num", IntegerType, true),
          ("expected", BooleanType, true)
        )
      ).transform(withIsOdd("num", "is_num_odd"))

      assertColumnEquality(df, "is_num_odd", "expected")
    }
  }

  describe("writesHockeyPlayers") {
    it("writes out a Parquet file") {
      val outputPath = new java.io.File("tmp/hockey_players").getCanonicalPath
      writesHockeyPlayers(outputPath)

      val obtainedDF = spark.read.parquet(outputPath)
      val expectedDF = spark.createDF(
        List(
          ("leech", 2, false),
          ("hull", 16, false),
          ("crosby", 87, true)
        ), List(
          ("last_name", StringType, true),
          ("jersey_number", IntegerType, true),
          ("is_jersey_odd", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(obtainedDF, expectedDF)

      NioUtils.removeAll(outputPath)
    }
  }

}
