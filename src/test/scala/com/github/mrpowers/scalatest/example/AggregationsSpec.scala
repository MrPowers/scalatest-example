package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions._
import Aggregations._

class AggregationsSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  describe("totalGoalsAgg") {
    it("shows groupBy basics") {
      val goalsDF = spark.createDF(
        List(
          ("messi", 2),
          ("messi", 1),
          ("pele", 3),
          ("pele", 1)
        ), List(
          ("name", StringType, true),
          ("goals", IntegerType, true)
        )
      )

      goalsDF.show()

      val aggDF = goalsDF
        .groupBy("name")
        .agg(sum("goals"))

      aggDF.show()

      goalsDF
        .groupBy("name")
        .agg(sum("goals").as("total_goals"))
        .show()
    }

    it("shows how to test a groupBy operation") {
      val goalsDF = spark.createDF(
        List(
          ("messi", 2),
          ("messi", 1),
          ("pele", 3),
          ("pele", 1)
        ), List(
          ("name", StringType, true),
          ("goals", IntegerType, true)
        )
      )

      val resDF = goalsDF.transform(totalGoalsAgg())

      val expectedDF = spark.createDF(
        List(
          ("messi", 3L),
          ("pele", 4L)
        ), List(
          ("name", StringType, true),
          ("total_goals", LongType, true)
        )
      )

      assertSmallDataFrameEquality(resDF, expectedDF)
    }
  }

  describe("studentsAgg") {
    it("prints out DataFrames to show the input and desired output") {
      val studentsDF = spark.createDF(
        List(
          ("mario", "italy", "europe"),
          ("stefano", "italy", "europe"),
          ("victor", "spain", "europe"),
          ("li", "china", "asia"),
          ("yuki", "japan", "asia"),
          ("vito", "italy", "europe")
        ), List(
          ("name", StringType, true),
          ("country", StringType, true),
          ("continent", StringType, true)
        )
      )

      studentsDF.show()

      val aggDF = studentsDF
        .groupBy("continent", "country")
        .count()

      aggDF.show()
    }

    it("tests the studentsAgg function") {
      val studentsDF = spark.createDF(
        List(
          ("mario", "italy", "europe"),
          ("stefano", "italy", "europe"),
          ("victor", "spain", "europe"),
          ("li", "china", "asia"),
          ("yuki", "japan", "asia"),
          ("vito", "italy", "europe")
        ), List(
          ("name", StringType, true),
          ("country", StringType, true),
          ("continent", StringType, true)
        )
      )

      val aggDF = studentsDF.transform(studentsAgg())

      val expectedDF = spark.createDF(
        List(
          ("europe", "italy", 3L),
          ("europe", "spain", 1L),
          ("asia", "china", 1L),
          ("asia", "japan", 1L)
        ), List(
          ("continent", StringType, true),
          ("country", StringType, true),
          ("count", LongType, false)
        )
      )

      assertSmallDataFrameEquality(aggDF, expectedDF)
    }
  }

  describe("averageGoalsAgg") {
    it("prints stuff out to show the input and expected result") {
      val hockeyPlayersDF = spark.createDF(
        List(
          ("gretzky", 40, 102, 1990),
          ("gretzky", 41, 122, 1991),
          ("gretzky", 31, 90, 1992),
          ("messier", 33, 61, 1989),
          ("messier", 45, 84, 1991),
          ("messier", 35, 72, 1992),
          ("messier", 25, 66, 1993)
        ), List(
          ("name", StringType, true),
          ("goals", IntegerType, true),
          ("assists", IntegerType, true),
          ("season", IntegerType, true)
        )
      )

      hockeyPlayersDF.show()

      hockeyPlayersDF
        .where(col("season").isin("1991", "1992"))
        .groupBy("name")
        .agg(avg("goals"), avg("assists"))
        .show()

      hockeyPlayersDF
        .groupBy("name")
        .agg(avg("goals"), avg("assists").as("average_assists"))
        .where(col("average_assists") >= 100)
        .show()
    }

    it("performs some real assertions") {
      val hockeyPlayersDF = spark.createDF(
        List(
          ("gretzky", 40, 102, 1990),
          ("gretzky", 41, 122, 1991),
          ("gretzky", 31, 90, 1992),
          ("messier", 33, 61, 1989),
          ("messier", 45, 84, 1991),
          ("messier", 35, 72, 1992),
          ("messier", 25, 66, 1993)
        ), List(
          ("name", StringType, true),
          ("goals", IntegerType, true),
          ("assists", IntegerType, true),
          ("season", IntegerType, true)
        )
      )

      val aggDF = hockeyPlayersDF.transform(averageGoalsAgg())

      val expectedDF = spark.createDF(
        List(
          ("gretzky", 37.3, 104.6)
        ), List(
          ("name", StringType, true),
          ("avg(goals)", DoubleType, true),
          ("average_assists", DoubleType, true)
        )
      )

      assertApproximateDataFrameEquality(aggDF, expectedDF, 0.1)
    }
  }

}
