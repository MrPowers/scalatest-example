package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.sql.ColumnExt._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import TestSpeedTest._

class TestSpeedTestSpec
    extends FunSpec
    with ColumnComparer
    with DataFrameComparer
    with SparkSessionTestWrapper {

  describe("speedTest") {

    it("assertLargeDataFrameEquality approach") {
      val sourceDF = spark.createDF(
        List(
          ("  BOO     "),
          (" HOO   "),
          (null)
        ), List(
          ("cry", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "clean_cry",
        myLowerClean(col("cry"))
      )

      val expectedDF = spark.createDF(
        List(
          ("  BOO     ", "boo"),
          (" HOO   ", "hoo"),
          (null, null)
        ), List(
          ("cry", StringType, true),
          ("clean_cry", StringType, true)
        )
      )

      assertLargeDataFrameEquality(actualDF, expectedDF)
    }

    it("assertSmallDataFrameEquality approach") {
      val sourceDF = spark.createDF(
        List(
          ("  BOO     "),
          (" HOO   "),
          (null)
        ), List(
          ("cry", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "clean_cry",
        myLowerClean(col("cry"))
      )

      val expectedDF = spark.createDF(
        List(
          ("  BOO     ", "boo"),
          (" HOO   ", "hoo"),
          (null, null)
        ), List(
          ("cry", StringType, true),
          ("clean_cry", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("assertColumnEquality approach") {
      val df = spark.createDF(
        List(
          ("  BOO     ", "boo"),
          (" HOO   ", "hoo"),
          (null, null)
        ), List(
          ("cry", StringType, true),
          ("expected", StringType, true)
        )
      ).withColumn(
        "clean_cry",
        myLowerClean(col("cry"))
      )

      assertColumnEquality(df, "clean_cry", "expected")
    }

    it("evalString approach") {
      assert(myLowerClean(lit("  BOO     ")).evalString() === "boo")
      assert(myLowerClean(lit(" HOO   ")).evalString() === "hoo")
    }

  }


}
