package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import IdentifyingBottlenecks._

class IdentifyingBottlenecksSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer
    with ColumnComparer {

  describe("withRunEverything") {
    it("runs all the transformations") {
      val obtainedDF = spark.createDF(
        List(
          (1),
          (2),
          (null)
        ), List(
          ("num1", IntegerType, true)
        )
      ).transform(withRunEverything())

      val expectedDF = spark.createDF(
        List(
          (1, "c1", "c2", "c3"),
          (2, "c1", "c2", "c3"),
          (null, "c1", "c2", "c3")
        ), List(
          ("num1", IntegerType, true),
          ("col1", StringType, false),
          ("col2", StringType, false),
          ("col3", StringType, false)
        )
      )

      assertSmallDataFrameEquality(obtainedDF, expectedDF)
    }
  }

  describe("withCol1") {
    it("appends col1") {
      val df = spark.createDF(
        List(
          (1, "c1"),
          (2, "c1"),
          (null, "c1")
        ), List(
          ("num1", IntegerType, true),
          ("expected", StringType, true)
        )
      ).transform(withCol1())

      assertColumnEquality(df, "col1", "expected")
    }
  }

  describe("withCol2") {
    it("appends col2") {
      val df = spark.createDF(
        List(
          (1, "c2"),
          (2, "c2"),
          (null, "c2")
        ), List(
          ("num1", IntegerType, true),
          ("expected", StringType, true)
        )
      ).transform(withCol2())

      assertColumnEquality(df, "col2", "expected")
    }
  }

  describe("withCol3") {
    it("appends col3") {
      val df = spark.createDF(
        List(
          (1, "c3"),
          (2, "c3"),
          (null, "c3")
        ), List(
          ("num1", IntegerType, true),
          ("expected", StringType, true)
        )
      ).transform(withCol3())

      assertColumnEquality(df, "col3", "expected")
    }
  }

}
