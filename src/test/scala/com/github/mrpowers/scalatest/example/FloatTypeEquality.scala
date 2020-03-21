package com.github.mrpowers.scalatest.example

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer, DatasetComparer}
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import NumberHelpers._

class FloatTypeEquality
    extends FunSpec
    with ColumnComparer
    with DatasetComparer
    with SparkSessionTestWrapper {

//  it("shows that assertColumnEquality doesn't work for FloatType columns") {
//
//    val df = spark.createDF(
//      List(
//        (1f, 0.3333333333333333f),
//        (3f, 1f)
//      ), List(
//        ("float1", FloatType, true),
//        ("expected", FloatType, true)
//      )
//    ).transform(oneThird("float1", "one_third_float1"))
//
//    assertColumnEquality(df, "one_third_float1", "expected")
//
//  }

  it("shows that assertFloatTypeColumnEquality works for FloatType columns") {

    val df = spark.createDF(
      List(
        (1f, 0.3333333333333333f),
        (3f, 1f)
      ), List(
        ("float1", FloatType, true),
        ("expected", FloatType, true)
      )
    ).transform(oneThird("float1", "one_third_float1"))

    assertFloatTypeColumnEquality(df, "one_third_float1", "expected", 0.01f)

  }

  it("compares DataFrames with FloatType columns") {

    val actualDF = spark.createDF(
      List(
        (1.0),
        (3.0)
      ), List(
        ("double1", DoubleType, true)
      )
    ).transform(oneThird("double1", "one_third_double1"))

    val expectedDF = spark.createDF(
      List(
        (1.0, 0.3333333333333333),
        (3.0, 1.0)
      ), List(
        ("double1", DoubleType, true),
        ("one_third_double1", DoubleType, true)
      )
    )

    assertApproximateDataFrameEquality(actualDF, expectedDF, 0.01)

  }

}

