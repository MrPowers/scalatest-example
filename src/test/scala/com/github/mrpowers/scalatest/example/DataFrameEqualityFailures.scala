package com.github.mrpowers.scalatest.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._


class DataFrameEqualityFailures
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

//  it("fails when the rows aren't equal") {
//    val df1 = spark.createDF(
//      List(
//        ("frank", 44, "us"),
//        ("li", 30, "china"),
//        ("bob", 1, "uk"),
//        ("camila", 5, "peru"),
//        ("maria", 19, "colombia")
//      ),
//      List(
//        ("name", StringType, true),
//        ("age", IntegerType, true),
//        ("country", StringType, true)
//      )
//    )
//
//    val df2 = spark.createDF(
//      List(
//        ("frank", 44, "us"),
//        ("li", 30, "china"),
//        ("bob", 1, "france"),
//        ("camila", 5, "peru"),
//        ("maria", 19, "colombia")
//      ),
//      List(
//        ("name", StringType, true),
//        ("age", IntegerType, true),
//        ("country", StringType, true)
//      )
//    )
//
//    assertSmallDataFrameEquality(df1, df2)
//  }

//  it("throws an error if the DataFrames have different schemas") {
//    val sourceDF = spark.createDF(
//      List(
//        (1, 2, 3, 4),
//        (5, 6, 7, 8)
//      ),
//      List(
//        ("num1", IntegerType, true),
//        ("num2", IntegerType, true),
//        ("num3", IntegerType, true),
//        ("num4", IntegerType, true)
//      )
//    )
//
//    val expectedDF = spark.createDF(
//      List(
//        (1, 2, 3, "word"),
//        (5, 6, 7, "word")
//      ),
//      List(
//        ("num1", IntegerType, true),
//        ("num2", IntegerType, true),
//        ("num3", IntegerType, true),
//        ("word", StringType, true)
//      )
//    )
//
//    assertSmallDataFrameEquality(sourceDF, expectedDF)
//  }

}
