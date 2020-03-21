package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import ColumnFunctions._

class ColumnFunctionsSpec
    extends FunSpec
    with ColumnComparer
    with SparkSessionTestWrapper {

  describe("bestLowerRemoveAllWhitespace") {
    it("downcases and removes all whitespace in a string") {
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
        bestLowerRemoveAllWhitespace(col("cry"))
      )

      assertColumnEquality(df, "clean_cry", "expected")
    }
  }

  it("demonstrates demonstrates how substring handles null") {
    val df = spark.createDF(
      List(
        ("Matthew"),
        ("Maria"),
        (null, null)
      ), List(
        ("first_name", StringType, true)
      )
    ).withColumn(
      "short_first_name",
      substring(col("first_name"), 0, 3)
    )

    df.show()
  }

  it("demonstrates how soundex handles null") {
    val df = spark.createDF(
      List(
        ("Matthew"),
        ("Maria"),
        (null, null)
      ), List(
        ("first_name", StringType, true)
      )
    ).withColumn(
      "soundex_first_name",
      soundex(col("first_name"))
    )

    df.show()
  }

}
