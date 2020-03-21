package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import FilesystemReads._

class FilesystemReadsSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer with ColumnComparer {

  describe("testableCode1") {
    it("starts out with a visual display of the output") {
      val path = new java.io.File("src/test/resources/dogs.csv").getCanonicalPath
      println("***")
      println(path)
      val df = testableCode1(path)
      df.show()
    }

    it("performs a real assertion") {
      val path = new java.io.File("src/test/resources/dogs.csv").getCanonicalPath
      val df = testableCode1(path)

      val expectedDF = spark.createDF(
        List(
          ("tio", "1", "hello"),
          ("renzo", "2", "hello")
        ), List(
          ("first_name", StringType, true),
          ("age", StringType, true),
          ("hi", StringType, false)
        )
      )

      assertSmallDataFrameEquality(df, expectedDF)
    }
  }

  describe("testableCode2") {
    it("performs a real assertion") {
      val df = testableCode2()

      val expectedDF = spark.createDF(
        List(
          ("tio", "1", "hello"),
          ("renzo", "2", "hello")
        ), List(
          ("first_name", StringType, true),
          ("age", StringType, true),
          ("hi", StringType, false)
        )
      )

      assertSmallDataFrameEquality(df, expectedDF)
    }
  }

  describe("testableCode3") {
    it("can leverage the test CSV file") {
      val df = testableCode3()

      val expectedDF = spark.createDF(
        List(
          ("tio", "1", "hello"),
          ("renzo", "2", "hello")
        ), List(
          ("first_name", StringType, true),
          ("age", StringType, true),
          ("hi", StringType, false)
        )
      )

      assertSmallDataFrameEquality(df, expectedDF)
    }

    it("can be tested without an external CSV file") {
      val dogsDF = spark.createDF(
        List(
          ("tio", "1"),
          ("renzo", "1")
        ), List(
          ("first_name", StringType, true),
          ("age", StringType, true)
        )
      )

      val df = testableCode3()

      val expectedDF = spark.createDF(
        List(
          ("tio", "1", "hello"),
          ("renzo", "2", "hello")
        ), List(
          ("first_name", StringType, true),
          ("age", StringType, true),
          ("hi", StringType, false)
        )
      )

      assertSmallDataFrameEquality(df, expectedDF)
    }
  }

  describe("withHi") {
    it("appends a hi column to a DataFrame") {
      val df = spark.createDF(
        List(
          ("tio", "hello"),
          ("renzo", "hello")
        ), List(
          ("whatever", StringType, true),
          ("expected", StringType, true)
        )
      ).transform(withHi())

      assertColumnEquality(df, "hi", "expected")
    }
  }

}
