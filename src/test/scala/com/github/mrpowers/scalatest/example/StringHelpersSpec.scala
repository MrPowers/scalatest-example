package com.github.mrpowers.scalatest.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class StringHelpersSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  describe("withStartsWithA") {
    it("checks if a string starts with the letter a") {
      val sourceDF = spark.createDF(
        List(
          ("apple"),
          ("animation"),
          ("bill")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = sourceDF
        .transform(StringHelpers.withStartsWithA("word"))

      val expectedDF = spark.createDF(
        List(
          ("apple", true),
          ("animation", true),
          ("bill", false)
        ), List(
          ("word", StringType, true),
          ("starts_with_a", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

}
